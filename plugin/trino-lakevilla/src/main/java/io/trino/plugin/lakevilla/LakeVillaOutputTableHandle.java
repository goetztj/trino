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

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.Objects;

public class LakeVillaOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    private final String path;
    private final int transactionId;
    private final boolean[] levels;
    private final int tableId;
    private final String configPath;

    public LakeVillaOutputTableHandle(String path, int transactionId, boolean[] levels, int tableId)
    {
        this.path = path;
        this.transactionId = transactionId;
        this.levels = levels;
        this.tableId = tableId;
        this.configPath = "./lv.config";
    }

    public String getPath()
    {
        return path;
    }

    public String getConfigPath()
    {
        return configPath;
    }

    public int getTransactionId()
    {
        return transactionId;
    }

    public boolean[] getLevels()
    {
        return levels;
    }

    public int getTableId()
    {
        return tableId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LakeVillaOutputTableHandle)) {
            return false;
        }
        LakeVillaOutputTableHandle that = (LakeVillaOutputTableHandle) o;
        return transactionId == that.transactionId &&
                tableId == that.tableId &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, transactionId, tableId);
    }
}
