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
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.MergePage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static java.util.Objects.requireNonNull;

/**
 * Sink of UPDATE, MERGE and DELETE (row-level changes, {@code DELETE_ROW_AND_INSERT_ROW}):
 * <ul>
 * <li>inserted rows (new rows and the new versions of updated rows) are written as new data
 * files of the transaction, like INSERT ({@link LakeVillaPageSink});</li>
 * <li>deleted rows (and the old versions of updated rows) are collected by data file and
 * position and returned as fragments: {@link LakeVillaMetadata#finishMerge} rewrites every
 * affected data file once (LakeVilla's {@code remove_rows}, copy-on-write), also if several
 * sinks deleted rows of the same file.</li>
 * </ul>
 */
public class LakeVillaMergeSink
        implements ConnectorMergeSink
{
    private final int columnCount;
    private final LakeVillaPageSink insertSink;
    private final Map<String, LakeVillaRowIds.PositionList> deletions = new HashMap<>();

    public LakeVillaMergeSink(LakeVillaTransaction transaction, LakeVillaMergeTableHandle handle, int maxRowsPerFile)
    {
        requireNonNull(transaction, "transaction is null");
        this.columnCount = handle.insertHandle().columns().size();
        this.insertSink = new LakeVillaPageSink(transaction, handle.insertHandle(), maxRowsPerFile);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        if (page.getPositionCount() == 0) {
            return;
        }
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);
        // the deletions page: the data columns followed by the row id
        mergePage.getDeletionsPage().ifPresent(deletionsPage ->
                LakeVillaRowIds.addRowIds(deletionsPage.getBlock(deletionsPage.getChannelCount() - 1), deletions));
        mergePage.getInsertionsPage().ifPresent(insertSink::appendPage);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish().join();
        return CompletableFuture.completedFuture(LakeVillaRowIds.serialize(deletions).map(ImmutableList::of).orElse(ImmutableList.of()));
    }

    @Override
    public void abort()
    {
        deletions.clear();
        insertSink.abort();
    }
}
