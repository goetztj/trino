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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_ARROW_ERROR;
import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_NATIVE_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Pages of the rows LakeVilla read: Arrow IPC streams of the table's snapshot (a scan) or of
 * its data files one after the other (the scan of UPDATE, MERGE or DELETE, which adds the row
 * id column {@link LakeVillaMetadata#MERGE_ROW_ID}: data file and position of each row)
 */
public class LakeVillaPageSource
        implements ConnectorPageSource
{
    /**
     * Rows to read: a whole table, or a data file (whose rows get row ids)
     */
    public record Part(Optional<String> file, Supplier<byte[]> rows)
    {
        public Part
        {
            requireNonNull(file, "file is null");
            requireNonNull(rows, "rows is null");
        }
    }

    private final List<Part> parts;
    private final List<LakeVillaColumnHandle> columns;
    private final TupleDomain<ColumnHandle> keyConstraint;
    private final BufferAllocator allocator;

    // the part being read
    private int nextPart;
    private Optional<String> file = Optional.empty();
    private long streamSize;
    private ArrowStreamReader reader;
    private VectorSchemaRoot root;
    // position of each column in the Arrow schema, -1 for the row id
    private int[] fieldIndexes;
    // key column filter (DELETE/SELECT ... WHERE key IN (...)), -1 for none
    private int keyFieldIndex = -1;
    private LakeVillaColumnHandle keyColumn;
    private Domain keyDomain;
    // position of the next batch in the part (row ids)
    private long rowOffset;

    private boolean finished;
    private long completedBytes;
    private long completedPositions;

    public LakeVillaPageSource(List<Part> parts, List<LakeVillaColumnHandle> columns, TupleDomain<ColumnHandle> keyConstraint)
    {
        this.parts = ImmutableList.copyOf(requireNonNull(parts, "parts is null"));
        this.columns = requireNonNull(columns, "columns is null");
        this.keyConstraint = requireNonNull(keyConstraint, "keyConstraint is null");
        this.allocator = LakeVillaArrow.newAllocator("page-source");
    }

    private static boolean isRowId(LakeVillaColumnHandle column)
    {
        return column.name().equals(LakeVillaMetadata.MERGE_ROW_ID);
    }

    // opens the next part; false if there is none
    private boolean openNextPart()
    {
        if (nextPart == parts.size()) {
            return false;
        }
        Part part = parts.get(nextPart++);
        byte[] rows = part.rows().get();
        file = part.file();
        streamSize = rows.length;
        rowOffset = 0;
        try {
            reader = new ArrowStreamReader(new ByteArrayInputStream(rows), allocator);
            root = reader.getVectorSchemaRoot();
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Invalid Arrow data of LakeVilla", e);
        }
        List<Field> fields = root.getSchema().getFields();
        fieldIndexes = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            LakeVillaColumnHandle column = columns.get(i);
            if (isRowId(column)) {
                if (file.isEmpty()) {
                    throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "Row ids need the rows of a data file");
                }
                fieldIndexes[i] = -1;
            }
            else {
                fieldIndexes[i] = fieldIndex(fields, column);
            }
        }
        Map<ColumnHandle, Domain> domains = keyConstraint.getDomains().orElse(Map.of());
        if (!domains.isEmpty()) {
            Map.Entry<ColumnHandle, Domain> key = domains.entrySet().iterator().next();
            keyColumn = (LakeVillaColumnHandle) key.getKey();
            keyFieldIndex = fieldIndex(fields, keyColumn);
            keyDomain = key.getValue();
        }
        return true;
    }

    private void closePart()
    {
        if (reader != null) {
            completedBytes += streamSize;
            try {
                // the reader closes its vectors
                reader.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            reader = null;
            root = null;
            streamSize = 0;
        }
    }

    // the snapshot read may have another schema than the one the query was planned with
    private static int fieldIndex(List<Field> fields, LakeVillaColumnHandle column)
    {
        int ordinal = column.ordinalPosition();
        if (ordinal >= 0 && ordinal < fields.size() && fields.get(ordinal).getName().equalsIgnoreCase(column.name())) {
            return ordinal;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equalsIgnoreCase(column.name())) {
                return i;
            }
        }
        throw new TrinoException(LAKEVILLA_NATIVE_ERROR, "Column %s is not part of the table's snapshot (the schema changed concurrently)".formatted(column.name()));
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        while (true) {
            if (reader == null && !openNextPart()) {
                finished = true;
                return null;
            }
            try {
                if (reader.loadNextBatch()) {
                    break;
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            closePart();
        }

        int batchRows = root.getRowCount();
        int rowCount = batchRows;
        int[] rows = null;
        if (keyFieldIndex >= 0) {
            FieldVector keyVector = root.getVector(keyFieldIndex);
            rows = new int[batchRows];
            int selected = 0;
            for (int row = 0; row < batchRows; row++) {
                Object value = LakeVillaArrow.readValue(keyVector, keyColumn.type(), row);
                if (value != null && keyDomain.includesNullableValue(value)) {
                    rows[selected++] = row;
                }
            }
            rowCount = selected;
        }

        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blocks[i] = fieldIndexes[i] < 0
                    ? LakeVillaRowIds.rowIds(file.orElseThrow(), rows, rowCount, rowOffset)
                    : LakeVillaArrow.toBlock(root.getVector(fieldIndexes[i]), columns.get(i).type(), rows, rowCount);
        }
        rowOffset += batchRows;
        completedPositions += rowCount;
        return SourcePage.create(new Page(rowCount, blocks));
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (reader == null ? 0 : reader.bytesRead());
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getMemoryUsage()
    {
        return streamSize + allocator.getAllocatedMemory();
    }

    @Override
    public void close()
    {
        try (allocator) {
            closePart();
        }
    }
}
