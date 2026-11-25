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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.transaction.IsolationLevel;
import jakarta.annotation.PreDestroy;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_NATIVE_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * The object store of a LakeVilla catalog: the warehouse with its schemas and Delta tables.
 * <p>
 * Like Spark's warehouse directory, schema {@code s} is the prefix {@code <warehouse>s.db/}
 * and table {@code t} of {@code s} is {@code <warehouse>s.db/t/}; the tables of the schema
 * {@code default} are {@code <warehouse>t/}. A table is a prefix with a Delta log
 * ({@code _delta_log/}). There is no catalog service: schemas and tables are found by
 * listing the bucket.
 */
public class LakeVillaStore
{
    static final String DEFAULT_SCHEMA = "default";
    private static final String SCHEMA_SUFFIX = ".db/";
    private static final String DELTA_LOG = "/_delta_log/";

    private final String warehouse;
    private final String levels;
    private final boolean redo;
    private final long handle;
    private final String bucket;
    private final IsolationLevel isolation;

    @Inject
    public LakeVillaStore(LakeVillaConfig config)
    {
        LakeVillaNative.load(config.getNativeLibrary());
        this.warehouse = normalizeWarehouse(config.getWarehouse());
        this.levels = config.getLevels();
        this.redo = config.isRedo();
        this.handle = LakeVillaNative.openStore(config.getConfigPath());
        String[] info = LakeVillaNative.storeInfo(handle);
        this.bucket = info[0];
        // ISOLATION of the LakeVilla config
        this.isolation = "serializable".equals(info[1]) ? IsolationLevel.SERIALIZABLE : IsolationLevel.REPEATABLE_READ;
    }

    @PreDestroy
    public void close()
    {
        LakeVillaNative.closeStore(handle);
    }

    static String normalizeWarehouse(String warehouse)
    {
        String path = warehouse.strip();
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        return path;
    }

    long handle()
    {
        return handle;
    }

    String levels()
    {
        return levels;
    }

    boolean redo()
    {
        return redo;
    }

    String bucket()
    {
        return bucket;
    }

    /**
     * Isolation level of LakeVilla's transactions (ISOLATION of the LakeVilla config)
     */
    IsolationLevel isolation()
    {
        return isolation;
    }

    String schemaPath(String schema)
    {
        return DEFAULT_SCHEMA.equals(schema) ? warehouse : warehouse + schema + SCHEMA_SUFFIX;
    }

    String tablePath(SchemaTableName table)
    {
        return schemaPath(table.getSchemaName()) + table.getTableName() + "/";
    }

    public Set<String> listSchemas()
    {
        Set<String> schemas = new TreeSet<>();
        schemas.add(DEFAULT_SCHEMA);
        for (String key : LakeVillaNative.listObjects(handle, warehouse)) {
            schemaOf(key.substring(warehouse.length())).ifPresent(schemas::add);
        }
        return schemas;
    }

    public boolean schemaExists(String schema)
    {
        return DEFAULT_SCHEMA.equals(schema) || LakeVillaNative.listObjects(handle, schemaPath(schema)).length > 0;
    }

    /**
     * Tables of a schema (or of all schemas)
     */
    public Set<SchemaTableName> listTables(Optional<String> schema)
    {
        String prefix = schema.map(this::schemaPath).orElse(warehouse);
        ImmutableSet.Builder<SchemaTableName> tables = ImmutableSet.builder();
        for (String key : LakeVillaNative.listObjects(handle, prefix)) {
            tableOf(key.substring(warehouse.length()))
                    .filter(table -> schema.isEmpty() || schema.get().equals(table.getSchemaName()))
                    .ifPresent(tables::add);
        }
        return tables.build();
    }

    /**
     * Schema name of an object key relative to the warehouse
     */
    static Optional<String> schemaOf(String relativeKey)
    {
        int end = relativeKey.indexOf(SCHEMA_SUFFIX);
        if (end <= 0 || relativeKey.substring(0, end).contains("/")) {
            return Optional.empty();
        }
        return validName(relativeKey.substring(0, end));
    }

    /**
     * Table of an object key of its Delta log, relative to the warehouse
     */
    static Optional<SchemaTableName> tableOf(String relativeKey)
    {
        int log = relativeKey.indexOf(DELTA_LOG);
        if (log <= 0) {
            return Optional.empty();
        }
        String[] parts = relativeKey.substring(0, log).split("/", -1);
        if (parts.length == 1) {
            return validName(parts[0]).map(table -> new SchemaTableName(DEFAULT_SCHEMA, table));
        }
        if (parts.length == 2 && parts[0].endsWith(".db")) {
            Optional<String> schema = validName(parts[0].substring(0, parts[0].length() - 3));
            Optional<String> table = validName(parts[1]);
            if (schema.isPresent() && table.isPresent()) {
                return Optional.of(new SchemaTableName(schema.get(), table.get()));
            }
        }
        return Optional.empty();
    }

    // Trino names are lower case: other objects cannot be addressed
    private static Optional<String> validName(String name)
    {
        if (name.isEmpty() || !name.equals(name.toLowerCase(Locale.ENGLISH))) {
            return Optional.empty();
        }
        return Optional.of(name);
    }

    public void createSchema(String schema)
    {
        if (schemaExists(schema)) {
            throw new TrinoException(ALREADY_EXISTS, "Schema already exists: " + schema);
        }
        // like S3A's directory markers
        if (!LakeVillaNative.writeObject(handle, schemaPath(schema), new byte[0])) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "Unable to create the schema " + schema);
        }
    }

    public void dropSchemaMarker(String schema)
    {
        LakeVillaNative.deleteObject(handle, schemaPath(schema));
    }

    /**
     * Schema of the newest snapshot of the table, empty if there is no Delta table at the path
     */
    public Optional<Schema> tableSchema(String path)
    {
        byte[] schema = LakeVillaNative.tableSchema(handle, path);
        if (schema == null) {
            return Optional.empty();
        }
        return Optional.of(LakeVillaArrow.readSchema(schema));
    }

    public void createTable(SchemaTableName table, Schema schema, Optional<String> comment)
    {
        String path = tablePath(table);
        String error = LakeVillaNative.createTable(handle, levels, path, LakeVillaArrow.writeSchema(schema), comment.orElse(""));
        if (error == null) {
            return;
        }
        if (error.startsWith("AlreadyExists")) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + table);
        }
        if (error.startsWith("NotEmpty")) {
            throw new TrinoException(ALREADY_EXISTS, "Location of table %s is not empty: s3://%s/%s".formatted(table, bucket, path));
        }
        if (error.startsWith("Invalid")) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "LakeVilla cannot create table %s: %s".formatted(table, error));
        }
        throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "LakeVilla cannot create table %s: %s".formatted(table, error));
    }

    public void dropTable(SchemaTableName table, String path)
    {
        String error = LakeVillaNative.dropTable(handle, levels, path);
        if (error == null) {
            return;
        }
        if (error.startsWith("NotFound")) {
            throw new TrinoException(NOT_FOUND, "Table not found: " + table);
        }
        throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "LakeVilla cannot drop table %s: %s".formatted(table, error));
    }

    @Override
    public String toString()
    {
        return "s3://%s/%s".formatted(bucket, requireNonNull(warehouse));
    }
}
