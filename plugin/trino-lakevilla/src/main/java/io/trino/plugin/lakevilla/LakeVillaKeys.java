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
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Keys of LakeVilla's key-based operations: values of the first column of a table, as
 * strings (integers in decimal, dates as yyyy-MM-dd, strings as they are)
 */
final class LakeVillaKeys
{
    static final int MAX_KEYS = 1_000;

    private LakeVillaKeys() {}

    static boolean isKeyType(Type type)
    {
        return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT) ||
                type.equals(DATE) || type instanceof VarcharType;
    }

    /**
     * A domain of the key column that is a set of keys (WHERE key = ... or key IN (...))
     */
    static boolean isKeyDomain(Type type, Domain domain)
    {
        return isKeyType(type) && !domain.isNullAllowed() && values(domain).isPresent();
    }

    static List<String> keys(Type type, Domain domain)
    {
        return values(domain).orElseThrow().stream()
                .map(value -> toKey(type, value))
                .collect(toImmutableList());
    }

    // the values of a set of keys; Trino represents consecutive integers and dates as a range
    // (IN (2, 3) is [2, 3]), which is expanded
    private static Optional<Collection<Object>> values(Domain domain)
    {
        if (domain.getValues().isNone()) {
            return Optional.empty();
        }
        if (domain.getValues().isDiscreteSet()) {
            List<Object> values = domain.getValues().getDiscreteSet();
            return values.size() <= MAX_KEYS ? Optional.of(values) : Optional.empty();
        }
        return domain.getValues().tryExpandRanges(MAX_KEYS);
    }

    static String toKey(Type type, Object value)
    {
        if (type.equals(DATE)) {
            return LocalDate.ofEpochDay((Long) value).toString();
        }
        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        return Long.toString((Long) value);
    }
}
