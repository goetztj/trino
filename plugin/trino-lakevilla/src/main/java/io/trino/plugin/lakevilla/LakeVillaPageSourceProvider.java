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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LakeVillaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final LakeVillaTransactionManager transactionManager;

    @Inject
    public LakeVillaPageSourceProvider(LakeVillaTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        LakeVillaTableHandle tableHandle = (LakeVillaTableHandle) table;
        LakeVillaTransaction lakeVillaTransaction = transactionManager.get((LakeVillaTransactionHandle) transaction);
        String path = ((LakeVillaSplit) split).path();
        List<LakeVillaColumnHandle> lakeVillaColumns = columns.stream().map(LakeVillaColumnHandle.class::cast).collect(toImmutableList());

        List<LakeVillaPageSource.Part> parts;
        if (lakeVillaColumns.stream().anyMatch(column -> column.name().equals(LakeVillaMetadata.MERGE_ROW_ID))) {
            // the scan of UPDATE, MERGE or DELETE: the data files one by one, with the row ids
            // (file, position) of their rows
            parts = lakeVillaTransaction.listFiles(path).stream()
                    .map(file -> new LakeVillaPageSource.Part(Optional.of(file), () -> lakeVillaTransaction.readFile(path, file)))
                    .collect(toImmutableList());
        }
        else {
            // the rows of the table in the transaction's snapshot
            parts = List.of(new LakeVillaPageSource.Part(Optional.empty(), () -> lakeVillaTransaction.readTable(path)));
        }
        return new LakeVillaPageSource(parts, lakeVillaColumns, tableHandle.keyConstraint());
    }
}
