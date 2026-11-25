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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * The whole table (LakeVilla reads the snapshot of a table at once). The split runs on the
 * node of the transaction: the LakeVilla transaction lives in that node's memory.
 */
public record LakeVillaSplit(String path, List<HostAddress> addresses)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(LakeVillaSplit.class);

    public LakeVillaSplit
    {
        requireNonNull(path, "path is null");
        addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of("path", path);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(path) + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
