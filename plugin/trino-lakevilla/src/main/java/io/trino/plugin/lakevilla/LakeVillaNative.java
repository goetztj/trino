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

import io.airlift.log.Logger;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * JNI binding of liblakevilla.so ({@code LV/src/LakeVilla/TransactionManager/jni_trino.cpp}
 * of the LakeVilla repository). Native errors are thrown as {@link RuntimeException}.
 * Data is exchanged as Arrow IPC streams.
 */
final class LakeVillaNative
{
    private static final Logger log = Logger.get(LakeVillaNative.class);
    private static final String LIBRARY_FILE = "liblakevilla.so";

    private static boolean loaded;

    private LakeVillaNative() {}

    /**
     * Loads liblakevilla.so from (in this order) the configured path, the directory of the
     * plugin, or java.library.path. The catalogs of the plugin share its class loader and the
     * library; a native library cannot be loaded by a second class loader.
     */
    static synchronized void load(Optional<String> configuredPath)
    {
        if (loaded) {
            return;
        }
        try {
            if (configuredPath.isPresent()) {
                System.load(Path.of(configuredPath.get()).toAbsolutePath().toString());
            }
            else {
                Optional<Path> pluginLibrary = pluginDirectoryLibrary();
                if (pluginLibrary.isPresent()) {
                    System.load(pluginLibrary.get().toString());
                }
                else {
                    System.loadLibrary("lakevilla");
                }
            }
        }
        catch (UnsatisfiedLinkError e) {
            if (e.getMessage() != null && e.getMessage().contains("already loaded in another classloader")) {
                throw new IllegalStateException("liblakevilla.so is already loaded by another class loader (e.g. a catalog created with dynamic catalog management): " +
                        "use static catalogs (etc/catalog) for LakeVilla", e);
            }
            throw new IllegalStateException("Unable to load liblakevilla.so (set lakevilla.native-library, put it into the plugin directory or on java.library.path; " +
                    "its dependencies Arrow, Parquet and the AWS SDK must be found by the dynamic linker, e.g. through LD_LIBRARY_PATH): " + e.getMessage(), e);
        }
        loaded = true;
        log.info("Loaded the LakeVilla native library");
    }

    private static Optional<Path> pluginDirectoryLibrary()
    {
        try {
            Path location = Path.of(LakeVillaNative.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path directory = Files.isDirectory(location) ? location : location.getParent();
            Path library = directory.resolve(LIBRARY_FILE);
            return Files.isRegularFile(library) ? Optional.of(library) : Optional.empty();
        }
        catch (URISyntaxException | RuntimeException e) {
            return Optional.empty();
        }
    }

    // store: a LakeVilla configuration and an S3 connection for metadata operations

    static native long openStore(String configPath);

    static native void closeStore(long store);

    /**
     * @return {bucket, isolation ("serializable" or "snapshot")}
     */
    static native String[] storeInfo(long store);

    static native String[] listObjects(long store, String prefix);

    static native boolean writeObject(long store, String path, byte[] data);

    static native boolean deleteObject(long store, String path);

    /**
     * @return Arrow IPC stream with the schema of the table (no rows), null if there is no Delta table at the path
     */
    static native byte[] tableSchema(long store, String path);

    /**
     * @return null on success, otherwise "&lt;result&gt;: &lt;message&gt;" (AlreadyExists, NotEmpty, NotFound, Invalid, Error)
     */
    static native String createTable(long store, String levels, String path, byte[] arrowSchema, String comment);

    /**
     * @return null on success, otherwise "&lt;result&gt;: &lt;message&gt;"
     */
    static native String dropTable(long store, String levels, String path);

    static native int nextTransactionId();

    // transactions: a LakeVilla transaction manager that begins with its first table (table id 0)

    /**
     * @param redo level 0: redo the operations of a conflicting transaction instead of aborting it
     */
    static native long beginTransaction(long store, String levels, String firstTablePath, int transactionId, boolean redo);

    /**
     * @return table id, -1 if the table cannot be opened (LakeVilla aborts the transaction)
     */
    static native int openTable(long transaction, String path);

    static native byte[] readTable(long transaction, int tableId);

    /**
     * @return false if LakeVilla refuses the rows (schema mismatch, partitioned table, storage error)
     */
    static native boolean addRows(long transaction, int tableId, byte[] arrowRows);

    /**
     * Removes the rows whose first column equals the key.
     *
     * @return number of removed rows
     */
    static native long deleteKey(long transaction, int tableId, String key);

    /**
     * @return number of removed rows, -1 if unknown
     */
    static native long deleteAll(long transaction, int tableId);

    // row-level changes (UPDATE, MERGE, DELETE): a row is identified by its data file and its position in the file

    /**
     * @return paths of the data files of the table in the transaction's snapshot (including its own changes)
     */
    static native String[] listFiles(long transaction, int tableId);

    /**
     * @return Arrow IPC stream with the rows of a data file of the snapshot, in file order
     */
    static native byte[] readFile(long transaction, int tableId, String path);

    /**
     * Removes the rows at the positions of a data file: the other rows are written to a new data file (copy-on-write).
     *
     * @return number of removed rows
     */
    static native long removeRows(long transaction, int tableId, String path, long[] positions);

    /**
     * @return true if the transaction committed, false if LakeVilla aborted it (conflict)
     */
    static native boolean commit(long transaction, boolean readOnly);

    static native void abort(long transaction);

    static native void destroyTransaction(long transaction);
}
