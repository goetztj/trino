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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

final class TestLakeVillaConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LakeVillaConfig.class)
                .setConfigPath(null)
                .setLevels("01")
                .setWarehouse("wh/")
                .setRedo(false)
                .setNativeLibrary(null)
                .setMaxRowsPerFile(1_000_000));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("lakevilla.config-path", "/etc/trino/lvconfig.conf")
                .put("lakevilla.levels", "012")
                .put("lakevilla.warehouse", "warehouse/")
                .put("lakevilla.redo", "true")
                .put("lakevilla.native-library", "/opt/lakevilla/liblakevilla.so")
                .put("lakevilla.max-rows-per-file", "1000")
                .buildOrThrow();

        LakeVillaConfig expected = new LakeVillaConfig()
                .setConfigPath("/etc/trino/lvconfig.conf")
                .setLevels("012")
                .setWarehouse("warehouse/")
                .setRedo(true)
                .setNativeLibrary("/opt/lakevilla/liblakevilla.so")
                .setMaxRowsPerFile(1000);

        assertFullMapping(properties, expected);
    }

    @Test
    void testLevelsValidation()
    {
        for (String levels : new String[] {"none", "", "0", "1", "2", "01", "02", "12", "012"}) {
            assertValidates(new LakeVillaConfig().setConfigPath("lvconfig.conf").setLevels(levels));
        }
        for (String levels : new String[] {"3", "10", "0123", "all"}) {
            assertFailsValidation(new LakeVillaConfig().setConfigPath("lvconfig.conf").setLevels(levels), "levels", "must be 'none' or a combination of the levels 0, 1 and 2 in ascending order (e.g. 01, 2, 012)", jakarta.validation.constraints.Pattern.class);
        }
        assertFailsValidation(new LakeVillaConfig(), "configPath", "must not be null", jakarta.validation.constraints.NotNull.class);
    }
}
