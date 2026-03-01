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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class LakeVillaSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(LakeVillaSplit.class);

    private final boolean[] levels;
    private final String path;
    private final int transactionId;
    private final int tableId;
    private final String configPath;

    @JsonCreator
    public LakeVillaSplit(@JsonProperty("path") String path, @JsonProperty("id") int txnId, @JsonProperty("tblid") int tblId)
    {
        this.levels = new boolean[3];
        this.levels[0] = false;
        this.levels[1] = false;
        this.levels[2] = false;
        this.path = requireNonNull(path, "path is null");
        this.transactionId = requireNonNull(txnId, "txnId is null");
        this.tableId = requireNonNull(tblId, "tblId is null");
        this.configPath = "./lv.config";
    }

    @JsonProperty
    public boolean[] getLevels()
    {
        return levels;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public String getConfigPath()
    {
        return configPath;
    }

    @JsonProperty
    public int getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public int getTableId()
    {
        return tableId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("txnID", transactionId)
                .add("tblID", tableId)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path);
    }
}
