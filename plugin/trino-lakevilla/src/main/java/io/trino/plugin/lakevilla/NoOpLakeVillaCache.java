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

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Caches nothing: every lookup misses, notifications are ignored
 */
public class NoOpLakeVillaCache
        implements LakeVillaCache
{
    @Override
    public Optional<Schema> getTableSchema(String path)
    {
        return Optional.empty();
    }

    @Override
    public void tableSchemaLoaded(String path, Schema schema) {}

    @Override
    public Optional<byte[]> getTableRows(TableRead read)
    {
        return Optional.empty();
    }

    @Override
    public void tableRowsRead(TableRead read, byte[] rows) {}

    @Override
    public void tableWritten(UUID transaction, String path) {}

    @Override
    public void transactionCommitted(UUID transaction, Set<String> writtenTables) {}

    @Override
    public void transactionAborted(UUID transaction) {}

    @Override
    public void tableCreated(String path) {}

    @Override
    public void tableDropped(String path) {}
}
