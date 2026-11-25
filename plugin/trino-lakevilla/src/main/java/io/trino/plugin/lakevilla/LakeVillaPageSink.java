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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Writes the rows of INSERT and CREATE TABLE AS as data files of the LakeVilla transaction
 * (one file per {@code lakevilla.max-rows-per-file} rows). The files become visible when the
 * transaction commits.
 */
public class LakeVillaPageSink
        implements ConnectorPageSink
{
    private final LakeVillaTransaction transaction;
    private final String path;
    private final Schema schema;
    private final List<Type> types;
    private final int maxRowsPerFile;
    private final BufferAllocator allocator;

    private final List<Page> pages = new ArrayList<>();
    private int bufferedRows;
    private long bufferedBytes;
    private long writtenBytes;

    public LakeVillaPageSink(LakeVillaTransaction transaction, LakeVillaWriteHandle handle, int maxRowsPerFile)
    {
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.path = handle.path();
        List<LakeVillaColumnHandle> columns = handle.columns();
        // the Arrow fields of the written rows; LakeVilla conforms them to the table schema
        // (columns are matched by name, missing nullable columns are null)
        this.schema = LakeVillaTypes.toArrowSchema(columns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(column.type())
                        .build())
                .collect(toImmutableList()));
        this.types = columns.stream().map(LakeVillaColumnHandle::type).collect(toImmutableList());
        this.maxRowsPerFile = maxRowsPerFile;
        this.allocator = LakeVillaArrow.newAllocator("page-sink");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        int offset = 0;
        while (offset < page.getPositionCount()) {
            int length = Math.min(page.getPositionCount() - offset, maxRowsPerFile - bufferedRows);
            Page part = offset == 0 && length == page.getPositionCount() ? page : page.getRegion(offset, length);
            pages.add(part);
            bufferedRows += length;
            bufferedBytes += part.getRetainedSizeInBytes();
            offset += length;
            if (bufferedRows >= maxRowsPerFile) {
                flush();
            }
        }
        return NOT_BLOCKED;
    }

    private void flush()
    {
        if (bufferedRows == 0) {
            return;
        }
        byte[] rows = LakeVillaArrow.toIpc(schema, types, pages, allocator);
        transaction.addRows(path, rows);
        writtenBytes += rows.length;
        pages.clear();
        bufferedRows = 0;
        bufferedBytes = 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return bufferedBytes + allocator.getAllocatedMemory();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            flush();
        }
        finally {
            allocator.close();
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        // written data files are not part of the table unless the transaction commits
        pages.clear();
        allocator.close();
    }
}
