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

import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import static java.util.Objects.requireNonNull;

/**
 * Target of UPDATE, MERGE and DELETE (row-level changes)
 *
 * @param insertHandle the columns of the merge pages (all columns of the table, in table order)
 */
public record LakeVillaMergeTableHandle(LakeVillaTableHandle tableHandle, LakeVillaWriteHandle insertHandle)
        implements ConnectorMergeTableHandle
{
    public LakeVillaMergeTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(insertHandle, "insertHandle is null");
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
