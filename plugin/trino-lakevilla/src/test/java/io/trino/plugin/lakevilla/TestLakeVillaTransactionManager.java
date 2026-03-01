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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests the native API to Trino
public class TestLakeVillaTransactionManager
{
    @Test
    public void testBeginCommitLifecycle()
    {
        // enable markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, false};

        LakeVillaTransactionManager txn =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test1",
                        "./lv.config",
                        1);

        try {
            assertThat(txn.begin()).as("Transaction should begin successfully").isTrue();

            txn.commit(true);
        }
        finally {
            txn.close();
        }
    }

    @Test
    public void g0()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d1);

            byte[] d2 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d2);

            byte[] d3 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d3);

            txn1.commit(false);

            byte[] d4 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d4);

            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void g1a()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d1);

            byte[] d2 = txn2.readTable(0, 1);

            txn1.close();

            byte[] d3 = txn2.readTable(0, 1);

            txn2.commit(true);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void g1b()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d1);

            byte[] d2 = txn2.readTable(0, 1);

            byte[] d3 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d3);

            txn1.commit(false);

            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void g1c()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d1);

            byte[] d2 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d2);

            byte[] d3 = txn1.readTable(0, 1);
            byte[] d4 = txn2.readTable(0, 1);

            txn1.commit(false);

            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void otv()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        LakeVillaTransactionManager txn3 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        3);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn3.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d1);

            byte[] d2 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d2);

            byte[] d3 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d3);

            txn1.commit(false);

            txn3.readTable(0, 1);

            byte[] d4 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d4);

            txn3.readTable(0, 1);

            txn2.commit(false);

            txn3.readTable(0, 1);

            txn3.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
            txn3.close();
        }
    }

    @Test
    public void pmp()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);

            txn2.writeToTable(0, d1);

            txn2.commit(false);

            byte[] d2 = txn1.readTable(0, 1);

            txn1.commit(true);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void p4()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);

            byte[] d2 = txn2.readTable(0, 1);

            byte[] d3 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d3);

            byte[] d4 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d4);

            txn1.commit(false);

            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void gsingle()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);

            byte[] d2 = txn2.readTable(0, 1);

            byte[] d3 = txn2.readTable(0, 1);

            byte[] d4 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d4);

            byte[] d5 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d5);

            txn2.commit(false);

            byte[] d6 = txn1.readTable(0, 1);

            txn1.commit(true);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void g2item()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);

            byte[] d2 = txn2.readTable(0, 1);

            byte[] d3 = txn1.readTable(0, 1);
            txn1.writeToTable(0, d3);

            byte[] d4 = txn2.readTable(0, 1);
            txn2.writeToTable(0, d4);

            txn1.commit(false);

            //fails
            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }

    @Test
    public void g2()
    {
        // enable global version log, markers, sublogs, and dependency tracking (see LV library)
        boolean[] variant = new boolean[] {true, true, true};

        LakeVillaTransactionManager txn1 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        1);

        LakeVillaTransactionManager txn2 =
                new LakeVillaTransactionManager(
                        variant,
                        "warehouse/test",
                        "./lv.config",
                        2);

        try {
            assertThat(txn1.begin()).as("Transaction should begin successfully").isTrue();
            assertThat(txn2.begin()).as("Transaction should begin successfully").isTrue();

            byte[] d1 = txn1.readTable(0, 1);

            byte[] d2 = txn2.readTable(0, 1);

            txn1.writeToTable(0, d1);

            txn2.writeToTable(0, d2);

            txn1.commit(false);

            //fails to prevent lost update
            txn2.commit(false);
        }
        finally {
            txn1.close();
            txn2.close();
        }
    }
}
