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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;

public class LakeVillaConfig
{
    private String configPath;
    private String levels = "01";
    private String warehouse = "wh/";
    private boolean redo;
    private String nativeLibrary;
    private int maxRowsPerFile = 1_000_000;

    @NotNull
    public String getConfigPath()
    {
        return configPath;
    }

    @Config("lakevilla.config-path")
    @ConfigDescription("LakeVilla configuration file (lvconfig.conf): object store (HOST, USER, PASSWD, AWSREGION, BUCKET), global log paths and protocol settings")
    public LakeVillaConfig setConfigPath(String configPath)
    {
        this.configPath = configPath;
        return this;
    }

    @NotNull
    @Pattern(regexp = "none|0?1?2?", message = "must be 'none' or a combination of the levels 0, 1 and 2 in ascending order (e.g. 01, 2, 012)")
    public String getLevels()
    {
        return levels;
    }

    @Config("lakevilla.levels")
    @ConfigDescription("LakeVilla levels of the transactions: 0 = markers and sublogs, 1 = markers and dependency tracking, 2 = global version log (01 = LV[Write], 2 = LV[Read], 012 = LV[Hybrid], none)")
    public LakeVillaConfig setLevels(String levels)
    {
        this.levels = levels;
        return this;
    }

    @NotNull
    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("lakevilla.warehouse")
    @ConfigDescription("Path of the warehouse in the bucket: schema s is <warehouse><s>.db/, table t of s is <warehouse><s>.db/<t>/ (schema default: <warehouse><t>/), as Spark's warehouse directory")
    public LakeVillaConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public boolean isRedo()
    {
        return redo;
    }

    @Config("lakevilla.redo")
    @ConfigDescription("Level 0: redo the operations of a conflicting transaction against the newest snapshot instead of aborting it. " +
            "Only correct for transactions whose writes do not depend on their reads (blind writes)")
    public LakeVillaConfig setRedo(boolean redo)
    {
        this.redo = redo;
        return this;
    }

    public Optional<String> getNativeLibrary()
    {
        return Optional.ofNullable(nativeLibrary);
    }

    @Config("lakevilla.native-library")
    @ConfigDescription("Path of liblakevilla.so; default: the plugin directory, then java.library.path")
    public LakeVillaConfig setNativeLibrary(String nativeLibrary)
    {
        this.nativeLibrary = nativeLibrary;
        return this;
    }

    @Min(1)
    public int getMaxRowsPerFile()
    {
        return maxRowsPerFile;
    }

    @Config("lakevilla.max-rows-per-file")
    @ConfigDescription("Maximum number of rows of a data file written by INSERT and CREATE TABLE AS")
    public LakeVillaConfig setMaxRowsPerFile(int maxRowsPerFile)
    {
        this.maxRowsPerFile = maxRowsPerFile;
        return this;
    }
}
