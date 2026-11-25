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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestLakeVillaRowIds
{
    @Test
    void testRowIds()
    {
        // all rows of a batch at position 10 of the file, and selected rows (key filter)
        Map<String, LakeVillaRowIds.PositionList> deletions = new HashMap<>();
        LakeVillaRowIds.addRowIds(LakeVillaRowIds.rowIds("wh/t/a.parquet", null, 3, 10), deletions);
        LakeVillaRowIds.addRowIds(LakeVillaRowIds.rowIds("wh/t/b.parquet", new int[] {4, 7}, 2, 0), deletions);
        assertThat(deletions.get("wh/t/a.parquet").toArray()).containsExactly(10, 11, 12);
        assertThat(deletions.get("wh/t/b.parquet").toArray()).containsExactly(4, 7);

        // row ids as the engine passes them (e.g. dictionary encoded after a join)
        Block rowIds = LakeVillaRowIds.rowIds("wh/t/c.parquet", null, 3, 0);
        Map<String, LakeVillaRowIds.PositionList> fromDictionary = new HashMap<>();
        LakeVillaRowIds.addRowIds(DictionaryBlock.create(4, rowIds, new int[] {2, 0, 2, 1}), fromDictionary);
        assertThat(fromDictionary.get("wh/t/c.parquet").toArray()).containsExactly(2, 0, 2, 1);
    }

    @Test
    void testFragments()
    {
        // two sinks deleted rows of the same file: one sorted list without duplicates per file
        Map<String, LakeVillaRowIds.PositionList> sink1 = new HashMap<>();
        LakeVillaRowIds.addRowIds(LakeVillaRowIds.rowIds("wh/t/ä.parquet", new int[] {5, 1}, 2, 0), sink1);
        LakeVillaRowIds.addRowIds(LakeVillaRowIds.rowIds("wh/t/b.parquet", null, 2, 0), sink1);
        Map<String, LakeVillaRowIds.PositionList> sink2 = new HashMap<>();
        LakeVillaRowIds.addRowIds(LakeVillaRowIds.rowIds("wh/t/ä.parquet", new int[] {1, 3}, 2, 0), sink2);

        List<Slice> fragments = List.of(
                LakeVillaRowIds.serialize(sink1).orElseThrow(),
                LakeVillaRowIds.serialize(sink2).orElseThrow());
        Map<String, long[]> merged = LakeVillaRowIds.deserialize(fragments);
        assertThat(merged).containsOnlyKeys("wh/t/ä.parquet", "wh/t/b.parquet");
        assertThat(merged.get("wh/t/ä.parquet")).containsExactly(1, 3, 5);
        assertThat(merged.get("wh/t/b.parquet")).containsExactly(0, 1);

        // a sink without deletions returns no fragment
        assertThat(LakeVillaRowIds.serialize(new HashMap<>())).isEmpty();
        assertThat(LakeVillaRowIds.deserialize(List.of())).isEmpty();
    }
}
