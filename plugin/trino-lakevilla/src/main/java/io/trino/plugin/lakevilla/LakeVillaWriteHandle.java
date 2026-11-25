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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Target of INSERT and CREATE TABLE AS.
 *
 * @param path location of the table in the bucket
 * @param columns the columns of the written pages, in this order
 */
public record LakeVillaWriteHandle(String schemaName, String tableName, String path, List<LakeVillaColumnHandle> columns)
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    public LakeVillaWriteHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(path, "path is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }
}
