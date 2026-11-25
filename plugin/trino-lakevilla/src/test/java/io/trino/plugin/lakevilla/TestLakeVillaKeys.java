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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.lakevilla.LakeVillaKeys.MAX_KEYS;
import static io.trino.plugin.lakevilla.LakeVillaKeys.isKeyDomain;
import static io.trino.plugin.lakevilla.LakeVillaKeys.keys;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLakeVillaKeys
{
    @Test
    void testKeys()
    {
        // WHERE k = 7, WHERE k IN (1, 3, 5)
        assertKeys(Domain.singleValue(BIGINT, 7L), "7");
        assertKeys(Domain.multipleValues(BIGINT, List.of(1L, 3L, 5L)), "1", "3", "5");
        // consecutive integers are a range in Trino: IN (2, 3), BETWEEN 2 AND 4
        assertKeys(Domain.multipleValues(INTEGER, List.of(2L, 3L)), "2", "3");
        assertKeys(Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 2L, true, 4L, true)), false), "2", "3", "4");
        assertKeys(Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b c"))), "a", "b c");
        long day = LocalDate.of(2024, 2, 29).toEpochDay();
        assertKeys(Domain.multipleValues(DATE, List.of(day, day + 1)), "2024-02-29", "2024-03-01");
    }

    @Test
    void testNoKeys()
    {
        // NULL, open ranges, types that are not keys and too many keys
        assertThat(isKeyDomain(BIGINT, Domain.onlyNull(BIGINT))).isFalse();
        assertThat(isKeyDomain(BIGINT, Domain.create(ValueSet.of(BIGINT, 1L), true))).isFalse();
        assertThat(isKeyDomain(BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false))).isFalse();
        assertThat(isKeyDomain(VARCHAR, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("a"), true, utf8Slice("b"), true)), false))).isFalse();
        assertThat(isKeyDomain(DOUBLE, Domain.singleValue(DOUBLE, 1.0))).isFalse();
        assertThat(isKeyDomain(BIGINT, Domain.multipleValues(BIGINT, LongStream.range(0, MAX_KEYS + 1).map(i -> i * 2).boxed().toList()))).isFalse();
        assertThat(isKeyDomain(BIGINT, Domain.none(BIGINT))).isFalse();
    }

    private static void assertKeys(Domain domain, String... expected)
    {
        assertThat(isKeyDomain(domain.getType(), domain)).isTrue();
        assertThat(keys(domain.getType(), domain)).containsExactly(expected);
    }
}
