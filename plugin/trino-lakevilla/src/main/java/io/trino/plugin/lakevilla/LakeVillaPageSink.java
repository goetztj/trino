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
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class LakeVillaPageSink
        implements ConnectorPageSink
{
    private final LakeVillaTransactionHandle transaction;
    private final int tableID;
    private final List<Object[]> buffer = new ArrayList<>();
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public LakeVillaPageSink(LakeVillaOutputTableHandle handle, ConnectorTransactionHandle transaction)
    {
        this.transaction = (LakeVillaTransactionHandle) transaction;
        this.tableID = handle.getTableId();

        if (this.transaction.txn == null) {
            try {
                this.transaction.txn = new LakeVillaTransactionManager(
                        handle.getLevels(),
                        handle.getPath(),
                        handle.getConfigPath(),
                        handle.getTransactionId());
                this.transaction.txn.begin();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void appendRow(Object[] row)
    {
        buffer.add(row);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return completedFuture(null);
    }

    private void flush()
    {
        if (buffer.isEmpty()) {
            return;
        }

        try {
            Object[] firstRow = buffer.get(0);
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < firstRow.length; i++) {
                Object value = firstRow[i];
                Field field;
                if (value instanceof Integer) {
                    field = new Field("col" + i, FieldType.nullable(new ArrowType.Int(32, true)), null);
                }
                else if (value instanceof Double) {
                    field = new Field("col" + i, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
                }
                else if (value instanceof String) {
                    field = new Field("col" + i, FieldType.nullable(new ArrowType.Utf8()), null);
                }
                else {
                    throw new RuntimeException("Unsupported column type: " + value.getClass());
                }
                fields.add(field);
            }
            Schema schema = new Schema(fields);

            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            root.allocateNew();

            int rowIndex = 0;
            for (Object[] row : buffer) {
                for (int colIndex = 0; colIndex < row.length; colIndex++) {
                    Object value = row[colIndex];
                    if (value == null) {
                        root.getVector(colIndex).setNull(rowIndex);
                    }
                    else if (value instanceof Integer) {
                        ((IntVector) root.getVector(colIndex)).setSafe(rowIndex, (Integer) value);
                    }
                    else if (value instanceof Double) {
                        ((Float8Vector) root.getVector(colIndex)).setSafe(rowIndex, (Double) value);
                    }
                    else if (value instanceof String) {
                        ((VarCharVector) root.getVector(colIndex)).setSafe(rowIndex, ((String) value).getBytes(StandardCharsets.UTF_8));
                    }
                }
                rowIndex++;
            }
            root.setRowCount(buffer.size());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            byte[] arrowBytes = out.toByteArray();

            if (!this.transaction.txn.writeToTable(tableID, arrowBytes)) {
                System.err.println("failed writing via JNI; have you loaded the library correctly?");
            }
            else {
                buffer.clear();
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to flush buffer to Arrow stream", e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        flush();
        return completedFuture(null);
    }

    @Override
    public void abort()
    {
        buffer.clear();
    }
}
