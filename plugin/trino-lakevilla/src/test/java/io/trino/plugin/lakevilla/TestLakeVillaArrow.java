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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestLakeVillaArrow
{
    private static final DecimalType SHORT_DECIMAL = DecimalType.createDecimalType(10, 3);
    private static final DecimalType LONG_DECIMAL = DecimalType.createDecimalType(38, 6);

    private static final List<Type> TYPES = ImmutableList.of(
            BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, SHORT_DECIMAL, LONG_DECIMAL,
            VARCHAR, VARBINARY, DATE, TIMESTAMP_MICROS, TIMESTAMP_TZ_MICROS);

    // one row per array; Trino's native values (null: SQL NULL)
    private static final List<Object[]> ROWS = ImmutableList.of(
            new Object[] {true, -128L, 32767L, (long) Integer.MIN_VALUE, Long.MAX_VALUE, (long) Float.floatToIntBits(1.5f), 1e300,
                    -1234567891L, Int128.valueOf("-12345678901234567890123456789012123456"),
                    utf8Slice("héllo ✓"), wrappedBuffer(new byte[] {0, -1, 16}), -153_000L, -1L,
                    LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_714_972_089_123L, 456_000_000, UTC_KEY)},
            new Object[] {false, 0L, 0L, 0L, 0L, (long) Float.floatToIntBits(-0.25f), -2.5, 0L, Int128.ZERO,
                    utf8Slice(""), wrappedBuffer(new byte[0]), 2_932_896L, 9_223_372_036_854_775L,
                    LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1L, 999_000_000, UTC_KEY)},
            new Object[TYPES.size()]);

    @Test
    void testRoundTrip()
            throws IOException
    {
        List<ColumnMetadata> columns = new ArrayList<>();
        for (int i = 0; i < TYPES.size(); i++) {
            columns.add(new ColumnMetadata("c" + i, TYPES.get(i)));
        }
        Schema schema = LakeVillaTypes.toArrowSchema(columns);

        // two pages: the rows of the stream are the rows of all pages
        Page first = page(ROWS.subList(0, 2));
        Page second = page(ROWS.subList(2, 3));
        byte[] ipc;
        try (BufferAllocator allocator = LakeVillaArrow.newAllocator("test")) {
            ipc = LakeVillaArrow.toIpc(schema, TYPES, List.of(first, second), allocator);
        }

        try (BufferAllocator allocator = LakeVillaArrow.newAllocator("test");
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(LakeVillaTypes.toColumns(root.getSchema())).isEqualTo(LakeVillaTypes.toColumns(schema));
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(root.getRowCount()).isEqualTo(ROWS.size());
            for (int column = 0; column < TYPES.size(); column++) {
                Type type = TYPES.get(column);
                Block block = LakeVillaArrow.toBlock(root.getVector(column), type, null, root.getRowCount());
                for (int row = 0; row < ROWS.size(); row++) {
                    assertThat(readNativeValue(type, block, row))
                            .describedAs("column %s (%s), row %s", column, type, row)
                            .isEqualTo(ROWS.get(row)[column]);
                    assertThat(LakeVillaArrow.readValue(root.getVector(column), type, row)).isEqualTo(ROWS.get(row)[column]);
                }
            }
            // selected rows (key filter of the page source)
            Block selected = LakeVillaArrow.toBlock(root.getVector(4), BIGINT, new int[] {1, 2}, 2);
            assertThat(BIGINT.getLong(selected, 0)).isEqualTo(0L);
            assertThat(selected.isNull(1)).isTrue();
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    @Test
    void testSchemaRoundTrip()
    {
        List<ColumnMetadata> columns = List.of(
                ColumnMetadata.builder().setName("id").setType(BIGINT).setNullable(false).setComment(Optional.of("the key")).build(),
                ColumnMetadata.builder().setName("ts").setType(TIMESTAMP_TZ_MICROS).build(),
                ColumnMetadata.builder().setName("hidden").setType(BIGINT).setHidden(true).build());
        Schema schema = LakeVillaArrow.readSchema(LakeVillaArrow.writeSchema(LakeVillaTypes.toArrowSchema(columns)));

        List<LakeVillaColumnHandle> handles = LakeVillaTypes.toColumns(schema);
        assertThat(handles).containsExactly(
                new LakeVillaColumnHandle("id", BIGINT, 0, false, Optional.of("the key")),
                new LakeVillaColumnHandle("ts", TIMESTAMP_TZ_MICROS, 1, true, Optional.empty()));
    }

    @Test
    void testTypeMapping()
    {
        for (Type type : TYPES) {
            assertThat(LakeVillaTypes.toTrinoType(LakeVillaTypes.toArrowType(type))).contains(type);
            assertThat(LakeVillaTypes.toSupportedType(type)).isEmpty();
        }
        // types without an exact Delta equivalent are stored as the closest Delta type
        assertThat(LakeVillaTypes.toSupportedType(createVarcharType(10))).contains(VARCHAR);
        assertThat(LakeVillaTypes.toSupportedType(createCharType(3))).contains(VARCHAR);
        assertThat(LakeVillaTypes.toSupportedType(TIMESTAMP_MILLIS)).contains(TIMESTAMP_MICROS);
        assertThat(LakeVillaTypes.toSupportedType(TIMESTAMP_TZ_MILLIS)).contains(TIMESTAMP_TZ_MICROS);
        assertThatThrownBy(() -> LakeVillaTypes.toArrowType(TIME_MILLIS))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported column type: time(3)");
        assertThat(LakeVillaTypes.isSupported(TIME_MILLIS)).isFalse();
    }

    private static Page page(List<Object[]> rows)
    {
        Block[] blocks = new Block[TYPES.size()];
        for (int column = 0; column < TYPES.size(); column++) {
            Type type = TYPES.get(column);
            BlockBuilder builder = type.createBlockBuilder(null, rows.size());
            for (Object[] row : rows) {
                writeNativeValue(type, builder, row[column]);
            }
            blocks[column] = builder.build();
        }
        return new Page(rows.size(), blocks);
    }
}
