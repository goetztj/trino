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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Row ids of row-level changes (UPDATE, MERGE, DELETE): a row is identified by its data file
 * (path in the bucket) and its position in the file, as LakeVilla reads it
 * ({@code read_snapshot_file}) and removes it ({@code remove_rows}, copy-on-write).
 */
final class LakeVillaRowIds
{
    static final RowType ROW_ID_TYPE = RowType.from(List.of(field("file", VARCHAR), field("position", BIGINT)));

    private LakeVillaRowIds() {}

    /**
     * Row ids of the given rows of a data file
     *
     * @param rows positions of the rows in the batch, null for all rows
     * @param offset position of the batch's first row in the file
     */
    static Block rowIds(String file, int[] rows, int rowCount, long offset)
    {
        BlockBuilder files = VARCHAR.createBlockBuilder(null, rowCount);
        BlockBuilder positions = BIGINT.createBlockBuilder(null, rowCount);
        Slice path = utf8Slice(file);
        for (int i = 0; i < rowCount; i++) {
            VARCHAR.writeSlice(files, path);
            BIGINT.writeLong(positions, offset + (rows == null ? i : rows[i]));
        }
        return RowBlock.fromFieldBlocks(rowCount, new Block[] {files.build(), positions.build()});
    }

    /**
     * Adds the row ids of a block to the deleted positions per file
     */
    static void addRowIds(Block rowIds, Map<String, PositionList> deletions)
    {
        List<Block> fields = RowBlock.getRowFieldsFromBlock(rowIds);
        Block files = fields.get(0);
        Block positions = fields.get(1);
        for (int i = 0; i < rowIds.getPositionCount(); i++) {
            String file = VARCHAR.getSlice(files, i).toStringUtf8();
            deletions.computeIfAbsent(file, _ -> new PositionList()).add(BIGINT.getLong(positions, i));
        }
    }

    /**
     * Fragment of a merge sink: the deleted positions per file
     */
    static Optional<Slice> serialize(Map<String, PositionList> deletions)
    {
        if (deletions.isEmpty()) {
            return Optional.empty();
        }
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        output.writeInt(deletions.size());
        for (Map.Entry<String, PositionList> entry : deletions.entrySet()) {
            byte[] path = entry.getKey().getBytes(UTF_8);
            output.writeInt(path.length);
            output.writeBytes(path);
            long[] positions = entry.getValue().toArray();
            output.writeInt(positions.length);
            for (long position : positions) {
                output.writeLong(position);
            }
        }
        return Optional.of(output.slice());
    }

    /**
     * The deleted positions per file of all fragments (several sinks may delete rows of the
     * same file), sorted and without duplicates
     */
    static Map<String, long[]> deserialize(Collection<Slice> fragments)
    {
        Map<String, PositionList> deletions = new TreeMap<>();
        for (Slice fragment : fragments) {
            SliceInput input = fragment.getInput();
            int files = input.readInt();
            for (int i = 0; i < files; i++) {
                byte[] path = new byte[input.readInt()];
                input.readBytes(path);
                PositionList positions = deletions.computeIfAbsent(new String(path, UTF_8), _ -> new PositionList());
                int count = input.readInt();
                for (int j = 0; j < count; j++) {
                    positions.add(input.readLong());
                }
            }
        }
        Map<String, long[]> result = new TreeMap<>();
        deletions.forEach((file, positions) -> result.put(file, Arrays.stream(positions.toArray()).sorted().distinct().toArray()));
        return result;
    }

    /**
     * Growable list of row positions
     */
    static final class PositionList
    {
        private long[] positions = new long[16];
        private int size;

        void add(long position)
        {
            if (size == positions.length) {
                positions = Arrays.copyOf(positions, size * 2);
            }
            positions[size++] = position;
        }

        long[] toArray()
        {
            return Arrays.copyOf(positions, size);
        }

        long sizeInBytes()
        {
            return (long) positions.length * Long.BYTES;
        }
    }
}
