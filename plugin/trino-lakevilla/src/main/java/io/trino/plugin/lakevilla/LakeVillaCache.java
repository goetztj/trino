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

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Hook for caching in the LakeVilla connector. The connector calls it wherever it reads from
 * LakeVilla (lookups) and wherever tables change (notifications). The default implementation,
 * {@link NoOpLakeVillaCache}, caches nothing; an implementation is selected in
 * {@link LakeVillaModule}.
 * <p>
 * What happens without a cache (see the README, "Caching"):
 * <ul>
 * <li>Table schemas: loaded once per Trino transaction ({@link LakeVillaMetadata}); every
 * transaction replays the table's Delta log (from the newest checkpoint) to get it.</li>
 * <li>Table rows: every scan reads all data files of the table's snapshot from the object
 * store, also if the same transaction read the table before.</li>
 * </ul>
 * The scans of row-level changes (UPDATE, MERGE, DELETE with conditions other than the key)
 * read the table data file by data file ({@link LakeVillaTransaction#readFile}), with the
 * position of every row; they do not use this hook.
 * <p>
 * Things an implementation has to consider:
 * <ul>
 * <li><b>Freshness.</b> Other Trino transactions, other LakeVilla clients and other engines
 * (Spark, Trino's Delta Lake connector) change tables without calling this hook. Only the
 * changes of this connector are notified.</li>
 * <li><b>Snapshots.</b> A transaction reads the snapshot taken when it first accessed a table
 * ({@link LakeVillaTransaction#openTable}); its own writes are part of what it reads
 * ({@link TableRead#writtenByTransaction()}).</li>
 * <li><b>Isolation.</b> A hit of {@link #getTableRows} replaces LakeVilla's read of the table.
 * LakeVilla then does not know that the transaction read the table: with
 * {@code ISOLATION=serializable} it validates the tables a transaction read at commit, so the
 * transaction is not checked against concurrent changes of that table (lost updates, write
 * skew). The snapshot is still taken (and, with levels 0/1, the marker placed) before the
 * lookup.</li>
 * <li><b>Concurrency.</b> The hook is shared by all transactions of the catalog and called
 * from several threads.</li>
 * <li><b>Memory.</b> Rows are Arrow IPC streams of whole tables (up to 2 GB each).</li>
 * </ul>
 */
public interface LakeVillaCache
{
    // lookups: an empty result reads from LakeVilla, which is then reported to the *Loaded/*Read method

    /**
     * Schema of the newest snapshot of the table at the path, empty to load it from LakeVilla.
     * Called once per table and Trino transaction (also for tables that do not exist).
     */
    Optional<Schema> getTableSchema(String path);

    /**
     * The schema LakeVilla loaded for the table at the path (after a miss of {@link #getTableSchema})
     */
    void tableSchemaLoaded(String path, Schema schema);

    /**
     * Rows of a table in the snapshot of a transaction (Arrow IPC stream), empty to read them
     * with LakeVilla. Called for every scan of a table.
     */
    Optional<byte[]> getTableRows(TableRead read);

    /**
     * The rows LakeVilla read (after a miss of {@link #getTableRows})
     */
    void tableRowsRead(TableRead read, byte[] rows);

    // notifications: changes made through this connector

    /**
     * The transaction wrote to the table (INSERT, CREATE TABLE AS, DELETE, TRUNCATE); the
     * change becomes visible to others when the transaction commits
     */
    void tableWritten(UUID transaction, String path);

    /**
     * The transaction committed; its writes to these tables are visible now
     */
    void transactionCommitted(UUID transaction, Set<String> writtenTables);

    /**
     * The transaction was rolled back or LakeVilla aborted it; its writes are discarded
     */
    void transactionAborted(UUID transaction);

    /**
     * A table was created (not transactional: immediately visible)
     */
    void tableCreated(String path);

    /**
     * A table was dropped (not transactional: immediately visible)
     */
    void tableDropped(String path);

    /**
     * A scan of a table by a transaction
     *
     * @param path location of the table in the bucket
     * @param writtenByTransaction the transaction wrote to the table before: the rows contain
     * its uncommitted changes
     */
    record TableRead(UUID transaction, String path, boolean writtenByTransaction)
    {
        public TableRead
        {
            requireNonNull(transaction, "transaction is null");
            requireNonNull(path, "path is null");
        }
    }
}
