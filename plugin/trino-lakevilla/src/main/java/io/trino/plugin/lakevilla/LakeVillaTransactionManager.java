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
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_TRANSACTION_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * The LakeVilla transactions of the Trino transactions that began on this node.
 * <p>
 * A LakeVilla transaction lives in the memory of the node that began it (the coordinator).
 * Its reads and writes must therefore run on that node: the connector requires a Trino
 * deployment in which the coordinator also executes tasks
 * ({@code node-scheduler.include-coordinator=true}, e.g. a single-node server).
 */
public class LakeVillaTransactionManager
{
    private final LakeVillaStore store;
    private final LakeVillaCache cache;
    private final Map<UUID, LakeVillaTransaction> transactions = new ConcurrentHashMap<>();

    @Inject
    public LakeVillaTransactionManager(LakeVillaStore store, LakeVillaCache cache)
    {
        this.store = requireNonNull(store, "store is null");
        this.cache = requireNonNull(cache, "cache is null");
    }

    public LakeVillaTransactionHandle begin()
    {
        LakeVillaTransaction transaction = new LakeVillaTransaction(UUID.randomUUID(), store, cache);
        transactions.put(transaction.uuid(), transaction);
        return new LakeVillaTransactionHandle(transaction.uuid());
    }

    public LakeVillaTransaction get(LakeVillaTransactionHandle handle)
    {
        LakeVillaTransaction transaction = transactions.get(handle.uuid());
        if (transaction == null) {
            throw new TrinoException(LAKEVILLA_TRANSACTION_NOT_FOUND, "LakeVilla transaction %s is not known on this node: the LakeVilla connector runs the reads and writes of a transaction on the node that began it; use a single-node Trino or set node-scheduler.include-coordinator=true"
                    .formatted(handle.uuid()));
        }
        return transaction;
    }

    public void commit(LakeVillaTransactionHandle handle)
    {
        LakeVillaTransaction transaction = transactions.remove(handle.uuid());
        if (transaction != null) {
            transaction.commit();
        }
    }

    public void rollback(LakeVillaTransactionHandle handle)
    {
        LakeVillaTransaction transaction = transactions.remove(handle.uuid());
        if (transaction != null) {
            transaction.rollback();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        transactions.values().forEach(LakeVillaTransaction::rollback);
        transactions.clear();
    }
}
