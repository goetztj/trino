/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.lakevilla;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Metadata of a LakeVilla catalog within one Trino transaction
 */
public class LakeVillaMetadata
        implements ConnectorMetadata
{
    static final String MERGE_ROW_ID = "$merge_row_id";

    private final LakeVillaStore store;
    private final LakeVillaTransaction transaction;
    private final LakeVillaCache cache;
    // table path -> columns (schema of the newest snapshot when the table was first used)
    private final Map<String, Optional<List<LakeVillaColumnHandle>>> columnsCache = new HashMap<>();

    public LakeVillaMetadata(LakeVillaStore store, LakeVillaTransaction transaction, LakeVillaCache cache)
    {
        this.store = requireNonNull(store, "store is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.cache = requireNonNull(cache, "cache is null");
    }

    // schemas

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return store.schemaExists(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(store.listSchemas());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        store.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (LakeVillaStore.DEFAULT_SCHEMA.equals(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "The schema default (the warehouse) cannot be dropped");
        }
        Set<SchemaTableName> tables = store.listTables(Optional.of(schemaName));
        if (!tables.isEmpty() && !cascade) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        for (SchemaTableName table : tables) {
            dropTable(table, store.tablePath(table));
        }
        store.dropSchemaMarker(schemaName);
    }

    // tables

    @Override
    public LakeVillaTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        String path = store.tablePath(tableName);
        if (columns(path).isEmpty()) {
            return null;
        }
        return new LakeVillaTableHandle(tableName.getSchemaName(), tableName.getTableName(), path);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        return new ConnectorTableMetadata(table.toSchemaTableName(), columnMetadata(existingColumns(table)));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return ImmutableList.copyOf(store.listTables(schemaName));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        for (LakeVillaColumnHandle column : existingColumns((LakeVillaTableHandle) tableHandle)) {
            // Trino's column names are lower case (Delta's are case-insensitive)
            handles.put(column.name().toLowerCase(ENGLISH), column);
        }
        return handles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((LakeVillaColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable().isPresent()
                ? List.of(prefix.toSchemaTableName())
                : listTables(session, prefix.getSchema());
        ImmutableList.Builder<TableColumnsMetadata> result = ImmutableList.builder();
        for (SchemaTableName table : tables) {
            // tables can disappear during the listing
            columns(store.tablePath(table)).ifPresent(columns -> result.add(TableColumnsMetadata.forTable(table, columnMetadata(columns))));
        }
        return result.build().iterator();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName table = tableMetadata.getTable();
        if (!store.schemaExists(table.getSchemaName())) {
            throw new SchemaNotFoundException(table.getSchemaName());
        }
        if (columns(store.tablePath(table)).isPresent()) {
            if (saveMode == SaveMode.IGNORE) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + table);
        }
        createTable(tableMetadata);
    }

    private Schema createTable(ConnectorTableMetadata tableMetadata)
    {
        if (!tableMetadata.getProperties().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support table properties");
        }
        SchemaTableName table = tableMetadata.getTable();
        Schema schema = LakeVillaTypes.toArrowSchema(tableMetadata.getColumns());
        // a Delta table written directly to the object store (not transactional)
        store.createTable(table, schema, tableMetadata.getComment());
        columnsCache.remove(store.tablePath(table));
        cache.tableCreated(store.tablePath(table));
        return schema;
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        dropTable(table.toSchemaTableName(), table.path());
    }

    private void dropTable(SchemaTableName table, String path)
    {
        store.dropTable(table, path);
        columnsCache.remove(path);
        cache.tableDropped(path);
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        return LakeVillaTypes.toSupportedType(type);
    }

    // writes

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        checkNoRetries(retryMode);
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName table = tableMetadata.getTable();
        if (!store.schemaExists(table.getSchemaName())) {
            throw new SchemaNotFoundException(table.getSchemaName());
        }
        createTable(tableMetadata);
        // the table exists from now on; it is dropped again if the transaction does not commit
        transaction.tableCreated(table);
        String path = store.tablePath(table);
        List<LakeVillaColumnHandle> columns = existingColumns(path, table);
        transaction.openTable(path);
        return new LakeVillaWriteHandle(table.getSchemaName(), table.getTableName(), path, columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        checkNoRetries(retryMode);
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        // takes the snapshot of the table (and reserves its next version with markers)
        transaction.openTable(table.path());
        return new LakeVillaWriteHandle(
                table.schemaName(),
                table.tableName(),
                table.path(),
                columns.stream().map(LakeVillaColumnHandle.class::cast).collect(toImmutableList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    // DELETE ... WHERE <key column> IN (...) and DELETE without WHERE use LakeVilla's
    // key-based removal and the removal of all files

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isEmpty()) {
            return Optional.empty();
        }
        Optional<ColumnHandle> keyColumn = domains.get().keySet().stream()
                .filter(column -> ((LakeVillaColumnHandle) column).ordinalPosition() == 0)
                .findFirst();
        if (keyColumn.isEmpty()) {
            return Optional.empty();
        }
        LakeVillaColumnHandle key = (LakeVillaColumnHandle) keyColumn.get();
        Domain domain = domains.get().get(key);
        if (!LakeVillaKeys.isKeyDomain(key.type(), domain)) {
            return Optional.empty();
        }
        TupleDomain<ColumnHandle> keyConstraint = table.keyConstraint().intersect(TupleDomain.withColumnDomains(Map.of(key, domain)));
        if (keyConstraint.equals(table.keyConstraint())) {
            return Optional.empty();
        }
        TupleDomain<ColumnHandle> remaining = constraint.getSummary().filter((column, _) -> !column.equals(key));
        return Optional.of(new ConstraintApplicationResult<>(table.withKeyConstraint(keyConstraint), remaining, constraint.getExpression(), false));
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    // row-level changes: UPDATE, MERGE and DELETE with other conditions than the key. Trino
    // deletes the old version of a row and inserts the new one; a row is identified by its data
    // file and its position in the file. The deleted rows are removed from their files at the
    // end (finishMerge): each affected file is rewritten without them (copy-on-write).

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new LakeVillaColumnHandle(MERGE_ROW_ID, LakeVillaRowIds.ROW_ID_TYPE, -1, false, Optional.empty());
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        checkNoRetries(retryMode);
        if (store.redo()) {
            // a redo against a newer snapshot cannot repeat the removal of rows of a file
            // that is gone in it
            throw new TrinoException(NOT_SUPPORTED, "UPDATE, MERGE and DELETE with conditions other than the key need lakevilla.redo=false");
        }
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        // takes the snapshot of the table (and reserves its next version with markers)
        transaction.openTable(table.path());
        LakeVillaWriteHandle insertHandle = new LakeVillaWriteHandle(table.schemaName(), table.tableName(), table.path(), existingColumns(table));
        return new LakeVillaMergeTableHandle(table, insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        LakeVillaMergeTableHandle handle = (LakeVillaMergeTableHandle) mergeTableHandle;
        String path = handle.tableHandle().path();
        // every affected data file is rewritten once, without its deleted rows
        LakeVillaRowIds.deserialize(fragments).forEach((file, positions) -> transaction.removeRows(path, file, positions));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        LakeVillaTableHandle table = (LakeVillaTableHandle) handle;
        if (table.keyConstraint().isNone()) {
            return OptionalLong.of(0);
        }
        if (table.keyConstraint().isAll()) {
            return transaction.deleteAll(table.path());
        }
        Map.Entry<ColumnHandle, Domain> key = table.keyConstraint().getDomains().orElseThrow().entrySet().iterator().next();
        List<String> keys = LakeVillaKeys.keys(((LakeVillaColumnHandle) key.getKey()).type(), key.getValue());
        return OptionalLong.of(transaction.deleteKeys(table.path(), keys));
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        transaction.deleteAll(((LakeVillaTableHandle) tableHandle).path());
    }

    // helpers

    private static void checkNoRetries(RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
    }

    private Optional<List<LakeVillaColumnHandle>> columns(String path)
    {
        return columnsCache.computeIfAbsent(path, _ -> tableSchema(path).map(LakeVillaTypes::toColumns));
    }

    // the schema of the newest snapshot: from the cache hook, otherwise from LakeVilla
    private Optional<Schema> tableSchema(String path)
    {
        Optional<Schema> cached = cache.getTableSchema(path);
        if (cached.isPresent()) {
            return cached;
        }
        Optional<Schema> schema = store.tableSchema(path);
        schema.ifPresent(loaded -> cache.tableSchemaLoaded(path, loaded));
        return schema;
    }

    private List<LakeVillaColumnHandle> existingColumns(LakeVillaTableHandle table)
    {
        return existingColumns(table.path(), table.toSchemaTableName());
    }

    private List<LakeVillaColumnHandle> existingColumns(String path, SchemaTableName table)
    {
        return columns(path).orElseThrow(() -> new TableNotFoundException(table));
    }

    private static List<ColumnMetadata> columnMetadata(List<LakeVillaColumnHandle> columns)
    {
        return columns.stream().map(LakeVillaColumnHandle::toColumnMetadata).collect(toImmutableList());
    }
}
