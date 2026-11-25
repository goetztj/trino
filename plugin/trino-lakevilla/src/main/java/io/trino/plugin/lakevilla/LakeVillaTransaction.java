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
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_NATIVE_ERROR;
import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_TRANSACTION_ABORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static java.util.Objects.requireNonNull;

/**
 * The LakeVilla transaction of a Trino transaction.
 * <p>
 * The native transaction begins with the first table the Trino transaction reads or writes;
 * further tables join it when they are accessed (LakeVilla's multi-table transactions). All
 * reads and writes of a Trino transaction (all of its statements) are part of one LakeVilla
 * transaction, which commits or aborts with the Trino transaction. LakeVilla's transaction
 * manager is not thread-safe: the calls are serialized.
 */
public class LakeVillaTransaction
{
    private static final Logger log = Logger.get(LakeVillaTransaction.class);

    private final UUID uuid;
    private final LakeVillaStore store;
    private final LakeVillaCache cache;
    private final int transactionId;

    private long handle;
    // table path -> LakeVilla table id
    private final Map<String, Integer> tables = new HashMap<>();
    // paths of the tables the transaction wrote to
    private final Set<String> writtenTables = new LinkedHashSet<>();
    private String abortReason;
    private boolean finished;
    // tables created by CREATE TABLE AS: dropped if the transaction does not commit
    private final List<SchemaTableName> createdTables = new ArrayList<>();

    public LakeVillaTransaction(UUID uuid, LakeVillaStore store, LakeVillaCache cache)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
        this.store = requireNonNull(store, "store is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.transactionId = LakeVillaNative.nextTransactionId();
    }

    public UUID uuid()
    {
        return uuid;
    }

    /**
     * Table id of the table in the LakeVilla transaction; the first access of a table takes its
     * snapshot (and, with levels 0/1, places a marker that reserves its next version)
     */
    public synchronized int openTable(String path)
    {
        checkActive();
        Integer id = tables.get(path);
        if (id != null) {
            return id;
        }
        if (handle == 0) {
            try {
                handle = LakeVillaNative.beginTransaction(store.handle(), store.levels(), path, transactionId, store.redo());
            }
            catch (RuntimeException e) {
                throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "Unable to begin a LakeVilla transaction on s3://%s/%s: %s".formatted(store.bucket(), path, e.getMessage()), e);
            }
            tables.put(path, 0);
            return 0;
        }
        int tableId = LakeVillaNative.openTable(handle, path);
        if (tableId < 0) {
            // LakeVilla aborted the transaction (its markers are released)
            abortReason = "LakeVilla could not open s3://%s/%s and aborted the transaction".formatted(store.bucket(), path);
            throw new TrinoException(LAKEVILLA_TRANSACTION_ABORTED, abortReason);
        }
        tables.put(path, tableId);
        return tableId;
    }

    /**
     * Rows of the table in the transaction's snapshot (Arrow IPC stream). The snapshot is taken
     * first; then the cache is asked (see {@link LakeVillaCache} for the consequences of a hit).
     */
    public synchronized byte[] readTable(String path)
    {
        int tableId = openTable(path);
        LakeVillaCache.TableRead read = new LakeVillaCache.TableRead(uuid, path, writtenTables.contains(path));
        Optional<byte[]> cached = cache.getTableRows(read);
        if (cached.isPresent()) {
            return cached.get();
        }
        byte[] rows;
        try {
            rows = LakeVillaNative.readTable(handle, tableId);
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
        cache.tableRowsRead(read, rows);
        return rows;
    }

    // a write of the transaction to the table
    private void written(String path)
    {
        if (writtenTables.add(path)) {
            cache.tableWritten(uuid, path);
        }
    }

    public synchronized void addRows(String path, byte[] arrowRows)
    {
        int tableId = openTable(path);
        written(path);
        boolean added;
        try {
            added = LakeVillaNative.addRows(handle, tableId, arrowRows);
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
        if (!added) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "LakeVilla refused to write a data file to s3://%s/%s (schema mismatch, partitioned table or storage error, see the server log)".formatted(store.bucket(), path));
        }
    }

    /**
     * Removes the rows whose key (first column) is one of the keys
     *
     * @return number of removed rows
     */
    public synchronized long deleteKeys(String path, List<String> keys)
    {
        int tableId = openTable(path);
        written(path);
        long rows = 0;
        try {
            for (String key : keys) {
                rows += LakeVillaNative.deleteKey(handle, tableId, key);
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
        return rows;
    }

    /**
     * Paths of the data files of the table in the transaction's snapshot (scans of UPDATE,
     * MERGE and DELETE, which identify rows by their file and position)
     */
    public synchronized List<String> listFiles(String path)
    {
        int tableId = openTable(path);
        try {
            return List.of(LakeVillaNative.listFiles(handle, tableId));
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
    }

    /**
     * Rows of a data file of the snapshot in file order (Arrow IPC stream); not cached
     * ({@link LakeVillaCache} sees whole-table reads only)
     */
    public synchronized byte[] readFile(String path, String file)
    {
        int tableId = openTable(path);
        try {
            return LakeVillaNative.readFile(handle, tableId, file);
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
    }

    /**
     * Removes the rows at the positions of a data file of the snapshot (copy-on-write)
     *
     * @return number of removed rows
     */
    public synchronized long removeRows(String path, String file, long[] positions)
    {
        int tableId = openTable(path);
        written(path);
        try {
            return LakeVillaNative.removeRows(handle, tableId, file, positions);
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
    }

    public synchronized OptionalLong deleteAll(String path)
    {
        int tableId = openTable(path);
        written(path);
        long rows;
        try {
            rows = LakeVillaNative.deleteAll(handle, tableId);
        }
        catch (RuntimeException e) {
            throw new TrinoException(LAKEVILLA_NATIVE_ERROR, e.getMessage(), e);
        }
        return rows < 0 ? OptionalLong.empty() : OptionalLong.of(rows);
    }

    public synchronized void tableCreated(SchemaTableName table)
    {
        createdTables.add(table);
    }

    public synchronized void commit()
    {
        checkState(!finished, "transaction already finished");
        finished = true;
        boolean committed = false;
        try {
            if (abortReason != null) {
                throw new TrinoException(LAKEVILLA_TRANSACTION_ABORTED, abortReason);
            }
            if (handle == 0) {
                committed = true;
                return;
            }
            try {
                // without writes: the read-only hint releases the markers of the snapshot
                committed = LakeVillaNative.commit(handle, writtenTables.isEmpty());
            }
            catch (RuntimeException e) {
                throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "LakeVilla commit failed: " + e.getMessage(), e);
            }
            if (!committed) {
                throw new TrinoException(TRANSACTION_CONFLICT, "LakeVilla aborted the transaction: it conflicts with a concurrent transaction (isolation %s, levels %s); retry the transaction"
                        .formatted(store.isolation(), store.levels()));
            }
            createdTables.clear();
        }
        finally {
            if (committed) {
                cache.transactionCommitted(uuid, ImmutableSet.copyOf(writtenTables));
            }
            else {
                cache.transactionAborted(uuid);
            }
            release();
        }
    }

    public synchronized void rollback()
    {
        if (finished) {
            return;
        }
        finished = true;
        try {
            if (handle != 0 && abortReason == null) {
                LakeVillaNative.abort(handle);
            }
        }
        catch (RuntimeException e) {
            log.warn(e, "Unable to abort LakeVilla transaction %s", transactionId);
        }
        finally {
            cache.transactionAborted(uuid);
            release();
        }
    }

    // destroys the native transaction and drops the tables created by an unsuccessful transaction
    private void release()
    {
        if (handle != 0) {
            LakeVillaNative.destroyTransaction(handle);
            handle = 0;
        }
        for (SchemaTableName table : createdTables) {
            try {
                store.dropTable(table, store.tablePath(table));
                cache.tableDropped(store.tablePath(table));
            }
            catch (RuntimeException e) {
                log.warn(e, "Unable to drop table %s of an unsuccessful CREATE TABLE AS", table);
            }
        }
        createdTables.clear();
    }

    private void checkActive()
    {
        if (finished) {
            throw new TrinoException(LAKEVILLA_TRANSACTION_ABORTED, "The LakeVilla transaction has already finished");
        }
        if (abortReason != null) {
            throw new TrinoException(LAKEVILLA_TRANSACTION_ABORTED, abortReason);
        }
    }
}
