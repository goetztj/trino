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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LakeVillaSplitManager
        implements ConnectorSplitManager
{
    private final LakeVillaClient lakevillaClient;

    @Inject
    public LakeVillaSplitManager(LakeVillaClient lakevillaClient)
    {
        this.lakevillaClient = requireNonNull(lakevillaClient, "lakevillaClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        LakeVillaTableHandle tableHandle = (LakeVillaTableHandle) connectorTableHandle;
        LakeVillaTable table = lakevillaClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        List<ConnectorSplit> splits = new ArrayList<>();

        splits.add(new LakeVillaSplit(lakevillaClient.getTablePath(), 0, table.geTableID()));

        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
