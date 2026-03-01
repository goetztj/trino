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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LakeVillaClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final String tablePath;

    @Inject
    public LakeVillaClient(LakeVillaConfig config)
    {
        this.tablePath = config.getTablePath();
    }

    public Set<String> getSchemaNames()
    {
        return Collections.singleton("default"); // single schema
    }

    public Set<String> getTableNames(String schema)
    {
        if ("default".equals(schema)) {
            return Collections.singleton("my_table"); // single table
        }
        return Collections.emptySet();
    }

    public LakeVillaTable getTable(String schema, String tableName)
    {
        if ("default".equals(schema) && "my_table".equals(tableName)) {
            return new LakeVillaTable(
                    "my_table",
                    List.of(
                            new LakeVillaColumn("id", BigintType.BIGINT),
                            new LakeVillaColumn("name", VarcharType.VARCHAR)),
                    0);
        }
        return null;
    }

    public String getTablePath()
    {
        return tablePath;
    }
}
