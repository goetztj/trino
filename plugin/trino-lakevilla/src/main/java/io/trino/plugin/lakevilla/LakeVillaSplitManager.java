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
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LakeVillaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final LakeVillaTransactionManager transactionManager;

    @Inject
    public LakeVillaSplitManager(NodeManager nodeManager, LakeVillaTransactionManager transactionManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        LakeVillaTableHandle table = (LakeVillaTableHandle) tableHandle;
        if (table.keyConstraint().isNone()) {
            return new FixedSplitSource(List.of());
        }
        // the snapshot of the table is taken when the query is scheduled
        transactionManager.get((LakeVillaTransactionHandle) transaction).openTable(table.path());
        // the LakeVilla transaction lives on this node: the split must run here
        return new FixedSplitSource(new LakeVillaSplit(table.path(), List.of(nodeManager.getCurrentNode().getHostAndPort())));
    }
}
