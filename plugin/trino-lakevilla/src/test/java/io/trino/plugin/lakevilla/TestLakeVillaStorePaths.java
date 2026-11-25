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

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.lakevilla.LakeVillaStore.normalizeWarehouse;
import static io.trino.plugin.lakevilla.LakeVillaStore.schemaOf;
import static io.trino.plugin.lakevilla.LakeVillaStore.tableOf;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLakeVillaStorePaths
{
    @Test
    void testNormalizeWarehouse()
    {
        assertThat(normalizeWarehouse("wh")).isEqualTo("wh/");
        assertThat(normalizeWarehouse("wh/")).isEqualTo("wh/");
        assertThat(normalizeWarehouse("/a/b")).isEqualTo("a/b/");
        assertThat(normalizeWarehouse("")).isEqualTo("");
        assertThat(normalizeWarehouse("/")).isEqualTo("");
    }

    @Test
    void testTableOf()
    {
        // Spark's warehouse layout: <schema>.db/<table>/, tables of the default schema in the warehouse
        assertThat(tableOf("tpch.db/orders/_delta_log/00000000000000000000.json"))
                .contains(new SchemaTableName("tpch", "orders"));
        assertThat(tableOf("orders/_delta_log/00000000000000000010.checkpoint.parquet"))
                .contains(new SchemaTableName("default", "orders"));
        assertThat(tableOf("tpch.db/orders/_delta_log/123-4/000.sub"))
                .contains(new SchemaTableName("tpch", "orders"));
        // data files, other objects, nested tables and names Trino cannot address
        assertThat(tableOf("tpch.db/orders/part-0000.parquet")).isEmpty();
        assertThat(tableOf("_global/log.txt")).isEmpty();
        assertThat(tableOf("a/b/c/_delta_log/0.json")).isEmpty();
        assertThat(tableOf("tpch/orders/_delta_log/0.json")).isEmpty();
        assertThat(tableOf("TPCH.db/orders/_delta_log/0.json")).isEmpty();
        assertThat(tableOf("tpch.db/Orders/_delta_log/0.json")).isEmpty();
        assertThat(tableOf("_delta_log/0.json")).isEmpty();
    }

    @Test
    void testSchemaOf()
    {
        assertThat(schemaOf("tpch.db/")).contains("tpch");
        assertThat(schemaOf("tpch.db/orders/_delta_log/0.json")).contains("tpch");
        assertThat(schemaOf("orders/_delta_log/0.json")).isEqualTo(Optional.empty());
        assertThat(schemaOf("orders/x.db/y")).isEmpty();
        assertThat(schemaOf(".db/")).isEmpty();
        assertThat(schemaOf("Tpch.db/")).isEmpty();
    }
}
