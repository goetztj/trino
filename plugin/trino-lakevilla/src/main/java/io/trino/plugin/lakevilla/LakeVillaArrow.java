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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static io.trino.plugin.lakevilla.LakeVillaErrorCode.LAKEVILLA_ARROW_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

/**
 * Conversion between Trino pages and the Arrow IPC streams of LakeVilla
 */
final class LakeVillaArrow
{
    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
    private static final int MICROS_PER_MILLI = 1_000;
    private static final int PICOS_PER_MICRO = 1_000_000;

    private LakeVillaArrow() {}

    static BufferAllocator newAllocator(String name)
    {
        return ROOT_ALLOCATOR.newChildAllocator(name, 0, Long.MAX_VALUE);
    }

    static Schema readSchema(byte[] ipc)
    {
        try (BufferAllocator allocator = newAllocator("schema");
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            return reader.getVectorSchemaRoot().getSchema();
        }
        catch (IOException e) {
            throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Invalid Arrow schema of LakeVilla", e);
        }
    }

    static byte[] writeSchema(Schema schema)
    {
        try (BufferAllocator allocator = newAllocator("schema");
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.end();
            }
            return out.toByteArray();
        }
        catch (IOException e) {
            throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Unable to serialize the Arrow schema", e);
        }
    }

    // reading: Arrow -> Trino

    /**
     * Block of the given rows of an Arrow vector
     *
     * @param rows positions of the rows in the vector, null for all rows
     */
    static Block toBlock(FieldVector vector, Type type, int[] rows, int rowCount)
    {
        BlockBuilder builder = type.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            int row = rows == null ? i : rows[i];
            if (vector.isNull(row)) {
                builder.appendNull();
            }
            else {
                writeValue(vector, type, row, builder);
            }
        }
        return builder.build();
    }

    /**
     * Trino value of a row of an Arrow vector, as used by {@link io.trino.spi.predicate.Domain}
     */
    static Object readValue(FieldVector vector, Type type, int row)
    {
        if (vector.isNull(row)) {
            return null;
        }
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        writeValue(vector, type, row, builder);
        Block block = builder.build();
        return readNativeValue(type, block, 0);
    }

    private static void writeValue(FieldVector vector, Type type, int row, BlockBuilder builder)
    {
        switch (vector) {
            case BitVector bits -> BOOLEAN.writeBoolean(builder, bits.get(row) == 1);
            case TinyIntVector tinyInts -> TINYINT.writeLong(builder, tinyInts.get(row));
            case SmallIntVector smallInts -> SMALLINT.writeLong(builder, smallInts.get(row));
            case IntVector ints -> INTEGER.writeLong(builder, ints.get(row));
            case BigIntVector bigInts -> BIGINT.writeLong(builder, bigInts.get(row));
            case Float4Vector floats -> REAL.writeFloat(builder, floats.get(row));
            case Float8Vector doubles -> DOUBLE.writeDouble(builder, doubles.get(row));
            case DecimalVector decimals -> {
                DecimalType decimalType = (DecimalType) type;
                BigInteger unscaled = decimals.getObject(row).setScale(decimalType.getScale()).unscaledValue();
                if (decimalType.isShort()) {
                    decimalType.writeLong(builder, unscaled.longValueExact());
                }
                else {
                    decimalType.writeObject(builder, Int128.valueOf(unscaled));
                }
            }
            case VarCharVector strings -> type.writeSlice(builder, Slices.wrappedBuffer(strings.get(row)));
            case LargeVarCharVector strings -> type.writeSlice(builder, Slices.wrappedBuffer(strings.get(row)));
            case VarBinaryVector binaries -> type.writeSlice(builder, Slices.wrappedBuffer(binaries.get(row)));
            case LargeVarBinaryVector binaries -> type.writeSlice(builder, Slices.wrappedBuffer(binaries.get(row)));
            case DateDayVector dates -> DATE.writeLong(builder, dates.get(row));
            case TimeStampMicroTZVector timestamps -> {
                long micros = timestamps.get(row);
                TIMESTAMP_TZ_MICROS.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        floorDiv(micros, MICROS_PER_MILLI),
                        floorMod(micros, MICROS_PER_MILLI) * PICOS_PER_MICRO,
                        UTC_KEY));
            }
            case TimeStampMicroVector timestamps -> TIMESTAMP_MICROS.writeLong(builder, timestamps.get(row));
            default -> throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Unsupported Arrow vector %s for type %s".formatted(vector.getClass().getSimpleName(), type));
        }
    }

    // writing: Trino -> Arrow

    /**
     * Arrow IPC stream (one record batch) of Trino pages; the columns of the pages are the
     * fields of the schema, in this order
     */
    static byte[] toIpc(Schema schema, List<Type> types, List<Page> pages, BufferAllocator allocator)
    {
        int rowCount = pages.stream().mapToInt(Page::getPositionCount).sum();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            for (int column = 0; column < types.size(); column++) {
                FieldVector vector = root.getVector(column);
                vector.setInitialCapacity(rowCount);
                vector.allocateNew();
                Type type = types.get(column);
                int row = 0;
                for (Page page : pages) {
                    Block block = page.getBlock(column);
                    for (int position = 0; position < page.getPositionCount(); position++, row++) {
                        if (block.isNull(position)) {
                            vector.setNull(row);
                        }
                        else {
                            setValue(vector, type, block, position, row);
                        }
                    }
                }
                vector.setValueCount(rowCount);
            }
            root.setRowCount(rowCount);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
        catch (IOException e) {
            throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Unable to serialize the rows", e);
        }
    }

    private static void setValue(FieldVector vector, Type type, Block block, int position, int row)
    {
        switch (vector) {
            case BitVector bits -> bits.setSafe(row, BOOLEAN.getBoolean(block, position) ? 1 : 0);
            case TinyIntVector tinyInts -> tinyInts.setSafe(row, TINYINT.getByte(block, position));
            case SmallIntVector smallInts -> smallInts.setSafe(row, SMALLINT.getShort(block, position));
            case IntVector ints -> ints.setSafe(row, INTEGER.getInt(block, position));
            case BigIntVector bigInts -> bigInts.setSafe(row, BIGINT.getLong(block, position));
            case Float4Vector floats -> floats.setSafe(row, REAL.getFloat(block, position));
            case Float8Vector doubles -> doubles.setSafe(row, DOUBLE.getDouble(block, position));
            case DecimalVector decimals -> {
                DecimalType decimalType = (DecimalType) type;
                BigInteger unscaled = decimalType.isShort()
                        ? BigInteger.valueOf(decimalType.getLong(block, position))
                        : ((Int128) decimalType.getObject(block, position)).toBigInteger();
                decimals.setSafe(row, new BigDecimal(unscaled, decimalType.getScale()));
            }
            case VarCharVector _, VarBinaryVector _ -> {
                if (!(type instanceof VarcharType) && !type.equals(VARBINARY)) {
                    throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Unexpected type %s for %s".formatted(type, vector.getField()));
                }
                Slice slice = type.getSlice(block, position);
                ((BaseVariableWidthVector) vector).setSafe(row, slice.byteArray(), slice.byteArrayOffset(), slice.length());
            }
            case DateDayVector dates -> dates.setSafe(row, DATE.getInt(block, position));
            case TimeStampMicroTZVector timestamps -> {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
                timestamps.setSafe(row, timestamp.getEpochMillis() * MICROS_PER_MILLI + timestamp.getPicosOfMilli() / PICOS_PER_MICRO);
            }
            case TimeStampMicroVector timestamps -> timestamps.setSafe(row, TIMESTAMP_MICROS.getLong(block, position));
            default -> throw new TrinoException(LAKEVILLA_ARROW_ERROR, "Unsupported Arrow vector %s for type %s".formatted(vector.getClass().getSimpleName(), type));
        }
    }
}
