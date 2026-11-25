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

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class LakeVillaPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final LakeVillaTransactionManager transactionManager;
    private final int maxRowsPerFile;

    @Inject
    public LakeVillaPageSinkProvider(LakeVillaTransactionManager transactionManager, LakeVillaConfig config)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.maxRowsPerFile = config.getMaxRowsPerFile();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, (LakeVillaWriteHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, (LakeVillaWriteHandle) insertTableHandle);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        return new LakeVillaMergeSink(transactionManager.get((LakeVillaTransactionHandle) transactionHandle), (LakeVillaMergeTableHandle) mergeHandle, maxRowsPerFile);
    }

    private ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, LakeVillaWriteHandle handle)
    {
        return new LakeVillaPageSink(transactionManager.get((LakeVillaTransactionHandle) transactionHandle), handle, maxRowsPerFile);
    }
}
