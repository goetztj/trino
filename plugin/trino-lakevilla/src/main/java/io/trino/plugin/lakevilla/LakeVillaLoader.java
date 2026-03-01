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

public class LakeVillaLoader
{
    private LakeVillaLoader()
    {}

    public static void loadLibrary()
    {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();
        String libPath = null;

        if (os.contains("mac")) {
            if (arch.contains("x86_64")) {
                libPath = "plugin/trino-lakevilla/libs/macos/x86_64/liblakevilla.dylib";
            }
            else if (arch.contains("aarch64") || arch.contains("arm64")) {
                libPath = "plugin/trino-lakevilla/libs/macos/arm64/liblakevilla.dylib";
            }
        }
        else if (os.contains("linux")) {
            if (arch.contains("x86_64")) {
                libPath = "plugin/trino-lakevilla/libs/linux/x86_64/liblakevilla.so";
            }
            else if (arch.contains("aarch64") || arch.contains("arm64")) {
                libPath = "plugin/trino-lakevilla/libs/linux/arm64/liblakevilla.so";
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported OS/arch: " + os + " / " + arch);
        }

        System.load(System.getProperty("user.dir") + "/" + libPath);
        System.out.println("Loaded native library: " + libPath);
    }
}
