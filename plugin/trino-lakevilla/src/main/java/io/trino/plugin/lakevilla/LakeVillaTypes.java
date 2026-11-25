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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Types of LakeVilla tables. LakeVilla describes a Delta table by an Arrow schema
 * (LV/src/Helpers/DeltaSchema.hpp):
 * <pre>
 * Delta           Arrow                  Trino
 * boolean         bool                   boolean
 * byte            int8                   tinyint
 * short           int16                  smallint
 * integer         int32                  integer
 * long            int64                  bigint
 * float           float32                real
 * double          float64                double
 * decimal(p,s)    decimal128(p,s)        decimal(p,s)
 * string          utf8                   varchar
 * binary          binary                 varbinary
 * date            date32                 date
 * timestamp       timestamp[us, UTC]     timestamp(6) with time zone
 * timestamp_ntz   timestamp[us]          timestamp(6)
 * </pre>
 * Columns of other types (array, map, struct) are not exposed.
 */
final class LakeVillaTypes
{
    static final String COMMENT = "comment";

    private LakeVillaTypes() {}

    /**
     * Trino type of an Arrow field of a table schema, empty if the type is not supported
     */
    static Optional<Type> toTrinoType(ArrowType type)
    {
        return switch (type) {
            case ArrowType.Bool _ -> Optional.of(BOOLEAN);
            case ArrowType.Int intType when intType.getIsSigned() -> switch (intType.getBitWidth()) {
                case 8 -> Optional.of(TINYINT);
                case 16 -> Optional.of(SMALLINT);
                case 32 -> Optional.of(INTEGER);
                case 64 -> Optional.of(BIGINT);
                default -> Optional.empty();
            };
            case ArrowType.FloatingPoint floatingPoint -> switch (floatingPoint.getPrecision()) {
                case SINGLE -> Optional.of(REAL);
                case DOUBLE -> Optional.of(DOUBLE);
                case HALF -> Optional.empty();
            };
            case ArrowType.Decimal decimal when decimal.getBitWidth() == 128 && decimal.getPrecision() <= Decimals.MAX_PRECISION ->
                    Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
            case ArrowType.Utf8 _, ArrowType.LargeUtf8 _ -> Optional.of(VARCHAR);
            case ArrowType.Binary _, ArrowType.LargeBinary _ -> Optional.of(VARBINARY);
            case ArrowType.Date date when date.getUnit() == DateUnit.DAY -> Optional.of(DATE);
            case ArrowType.Timestamp timestamp when timestamp.getUnit() == TimeUnit.MICROSECOND ->
                    Optional.of(timestamp.getTimezone() == null ? TIMESTAMP_MICROS : TIMESTAMP_TZ_MICROS);
            default -> Optional.empty();
        };
    }

    /**
     * Arrow type of the Delta column of a Trino type
     */
    static ArrowType toArrowType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        if (type.equals(TINYINT)) {
            return new ArrowType.Int(8, true);
        }
        if (type.equals(SMALLINT)) {
            return new ArrowType.Int(16, true);
        }
        if (type.equals(INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        if (type.equals(BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        if (type.equals(REAL)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if (type.equals(DOUBLE)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (type instanceof DecimalType decimal) {
            return new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 128);
        }
        if (type instanceof VarcharType) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (type.equals(VARBINARY)) {
            return ArrowType.Binary.INSTANCE;
        }
        if (type.equals(DATE)) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    /**
     * Type LakeVilla stores for a Trino type that has no exact Delta equivalent
     * ({@link io.trino.spi.connector.ConnectorMetadata#getSupportedType})
     */
    static Optional<Type> toSupportedType(Type type)
    {
        if (type instanceof VarcharType varchar && !varchar.isUnbounded()) {
            return Optional.of(VARCHAR);
        }
        if (type instanceof CharType) {
            return Optional.of(VARCHAR);
        }
        if (type instanceof TimestampType timestamp && timestamp.getPrecision() != TIMESTAMP_MICROS.getPrecision()) {
            return Optional.of(TIMESTAMP_MICROS);
        }
        if (type instanceof TimestampWithTimeZoneType timestamp && timestamp.getPrecision() != TIMESTAMP_TZ_MICROS.getPrecision()) {
            return Optional.of(TIMESTAMP_TZ_MICROS);
        }
        return Optional.empty();
    }

    static boolean isSupported(Type type)
    {
        try {
            toArrowType(type);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    /**
     * Columns of a table schema; columns with unsupported types are left out
     */
    static List<LakeVillaColumnHandle> toColumns(Schema schema)
    {
        List<LakeVillaColumnHandle> columns = new ArrayList<>();
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int ordinal = i;
            toTrinoType(field.getType()).ifPresent(type -> columns.add(new LakeVillaColumnHandle(
                    field.getName(),
                    type,
                    ordinal,
                    field.isNullable(),
                    Optional.ofNullable(field.getMetadata()).map(metadata -> metadata.get(COMMENT)))));
        }
        return columns;
    }

    /**
     * Arrow schema of the columns of a new table
     */
    static Schema toArrowSchema(List<ColumnMetadata> columns)
    {
        List<Field> fields = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (column.isHidden()) {
                continue;
            }
            Map<String, String> metadata = column.getComment() == null ? null : Map.of(COMMENT, column.getComment());
            fields.add(new Field(column.getName(), new FieldType(column.isNullable(), toArrowType(column.getType()), null, metadata), null));
        }
        return new Schema(fields);
    }
}
