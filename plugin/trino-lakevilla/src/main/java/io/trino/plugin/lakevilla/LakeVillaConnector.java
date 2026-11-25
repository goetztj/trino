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
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/**
 * Trino connector for Delta tables accessed through LakeVilla.
 * <p>
 * Every Trino transaction is one LakeVilla transaction: the statements of a transaction
 * (START TRANSACTION ... COMMIT, or a single statement in auto-commit mode) read the snapshots
 * of the tables they access and commit their writes atomically, across tables, or abort on
 * conflicts.
 */
public class LakeVillaConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final LakeVillaStore store;
    private final LakeVillaTransactionManager transactionManager;
    private final LakeVillaCache cache;
    private final LakeVillaSplitManager splitManager;
    private final LakeVillaPageSourceProvider pageSourceProvider;
    private final LakeVillaPageSinkProvider pageSinkProvider;

    @Inject
    public LakeVillaConnector(
            LifeCycleManager lifeCycleManager,
            LakeVillaStore store,
            LakeVillaTransactionManager transactionManager,
            LakeVillaCache cache,
            LakeVillaSplitManager splitManager,
            LakeVillaPageSourceProvider pageSourceProvider,
            LakeVillaPageSinkProvider pageSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.store = requireNonNull(store, "store is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        // ISOLATION=serializable: SERIALIZABLE, ISOLATION=snapshot: REPEATABLE_READ (snapshot isolation)
        checkConnectorSupports(store.isolation(), isolationLevel);
        return transactionManager.begin();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return new LakeVillaMetadata(store, transactionManager.get((LakeVillaTransactionHandle) transactionHandle), cache);
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.commit((LakeVillaTransactionHandle) transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.rollback((LakeVillaTransactionHandle) transactionHandle);
    }

    // multi-statement write transactions are LakeVilla's purpose
    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return false;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return Set.of(NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
