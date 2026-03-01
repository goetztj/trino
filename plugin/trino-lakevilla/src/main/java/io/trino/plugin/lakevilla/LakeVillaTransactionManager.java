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

import io.trino.spi.connector.ConnectorTransactionHandle;

public class LakeVillaTransactionManager
        implements AutoCloseable, ConnectorTransactionHandle
{
    private long handle;

    private int nextTabeId;

    public LakeVillaTransactionManager(boolean[] levels, String path, String config, int transactionId)
    {
        handle = createNative(levels, path, config, transactionId);
    }

    public boolean begin()
    {
        return beginTransaction(handle);
    }

    public void commit(boolean readOnly)
    {
        commitNative(handle, readOnly);
    }

    public byte[] readTable(int tableId, int numThreads)
    {
        return readTableAsBytes(handle, tableId);
    }

    public int openNewTable(String path)
    {
        nextTabeId++;
        openTable(path);
        return nextTabeId;
    }

    public boolean writeToTable(int tableId, byte[] arrowTable)
    {
        return writeToTableNative(handle, tableId, arrowTable);
    }

    @Override
    public void close()
    {
        if (handle != 0) {
            destroyNative(handle);
            handle = 0;
        }
    }

    // Native methods
    private native long createNative(boolean[] levels, String path, String configPath, int transactionId);

    private native void destroyNative(long handle);

    private native long readTable(long handle, int tableId, int numThreads);

    private native byte[] readTableAsBytes(long handle, int tableId);

    private native boolean beginTransaction(long handle);

    private native boolean writeToTableNative(long handle, int tableId, byte[] arrowTable);

    private native void commitNative(long handle, boolean readOnly);

    private native void openTable(String path);

    static {
        LakeVillaLoader.loadLibrary();
    }
}
