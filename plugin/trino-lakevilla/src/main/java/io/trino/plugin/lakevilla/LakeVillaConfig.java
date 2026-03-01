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
import jakarta.validation.constraints.NotNull;

public class LakeVillaConfig
{
    private String lvconfigpath;
    private String tablepath;

    @NotNull
    public String getLVConfigPath()
    {
        return lvconfigpath;
    }

    @Config("lv-config-path")
    public LakeVillaConfig setLVConfigPath(String path)
    {
        this.lvconfigpath = path;
        return this;
    }

    @NotNull
    public String getTablePath()
    {
        return tablepath;
    }

    @Config("table-path")
    public LakeVillaConfig setTablePath(String tablepath)
    {
        this.tablepath = tablepath;
        return this;
    }
}
