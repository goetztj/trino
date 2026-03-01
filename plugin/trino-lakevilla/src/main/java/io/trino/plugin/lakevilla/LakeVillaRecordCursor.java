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
import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class LakeVillaRecordCursor
        implements RecordCursor
{
    private final List<LakeVillaColumnHandle> columns;
    private final ArrowStreamReader arrowReader;
    private final VectorSchemaRoot root;
    private final LakeVillaTransactionHandle transaction;

    private int currentRow;
    private int batchRowCount;

    public LakeVillaRecordCursor(LakeVillaSplit split, List<LakeVillaColumnHandle> columns, ConnectorTransactionHandle transaction)
    {
        this.columns = columns;

        this.transaction = (LakeVillaTransactionHandle) transaction;

        if (this.transaction.txn == null) {
            try {
                this.transaction.txn = new LakeVillaTransactionManager(
                        split.getLevels(),
                        split.getPath(),
                        split.getConfigPath(),
                        split.getTransactionId());
                this.transaction.txn.begin();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to initialize Arrow RecordCursor 1", e);
            }
        }

        try {
            byte[] arrowBytes = this.transaction.txn.readTable(split.getTableId(), 1);

            InputStream inputStream = new ByteArrayInputStream(arrowBytes);

            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            arrowReader = new ArrowStreamReader(inputStream, allocator);
            root = arrowReader.getVectorSchemaRoot();

            batchRowCount = arrowReader.loadNextBatch() ? root.getRowCount() : 0;
            currentRow = -1;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize Arrow RecordCursor", e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return arrowReader.bytesRead();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentRow++;
        if (currentRow >= batchRowCount) {
            try {
                // Load next batch
                if (!arrowReader.loadNextBatch()) {
                    return false; // no more rows
                }
                batchRowCount = root.getRowCount();
                currentRow = 0;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        FieldVector vector = root.getVector(field);
        if (vector.isNull(currentRow)) {
            throw new IllegalStateException("Value is null");
        }

        if (vector instanceof BitVector iv) {
            return iv.get(currentRow) == 1;
        }
        else {
            throw new UnsupportedOperationException("Unsupported type for getLong: " + vector.getClass());
        }
    }

    @Override
    public long getLong(int field)
    {
        FieldVector vector = root.getVector(field);
        if (vector.isNull(currentRow)) {
            throw new IllegalStateException("Value is null");
        }

        if (vector instanceof IntVector iv) {
            return iv.get(currentRow);
        }
        else if (vector instanceof BigIntVector bv) {
            return bv.get(currentRow);
        }
        else {
            throw new UnsupportedOperationException("Unsupported type for getLong: " + vector.getClass());
        }
    }

    @Override
    public double getDouble(int field)
    {
        FieldVector vector = root.getVector(field);

        if (vector.isNull(currentRow)) {
            throw new IllegalStateException("Value is null");
        }

        if (vector instanceof Float8Vector fv) {
            return fv.get(currentRow);
        }
        else if (vector instanceof Float4Vector fv) {
            return fv.get(currentRow);
        }

        throw new UnsupportedOperationException(
                "Unsupported type for DOUBLE: " + vector.getClass());
    }

    @Override
    public Slice getSlice(int field)
    {
        FieldVector vector = root.getVector(field);
        if (vector.isNull(currentRow)) {
            return null;
        }

        if (vector instanceof VarCharVector vv) {
            byte[] bytes = vv.get(currentRow);
            return Slices.wrappedBuffer(bytes);
        }
        else {
            throw new UnsupportedOperationException("Unsupported type for getSlice: " + vector.getClass());
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return root.getVector(field).isNull(currentRow);
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        try {
            arrowReader.close();
        }
        catch (IOException e) {
            // ignore
        }
        //txn.commit(true);
        //txn.close();
    }
}
