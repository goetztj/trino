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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_ISOLATION_LEVEL;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Hermitage (https://github.com/ept/hermitage, Kleppmann) for the LakeVilla connector: the
 * isolation anomalies of Adya's classification, run as concurrent SQL transactions over JDBC
 * against a running Trino server with a LakeVilla catalog.
 * <p>
 * Every Hermitage transaction is a JDBC connection (auto-commit off) with its own thread. A
 * statement that does not finish within {@link #BLOCKED_AFTER_MILLIS} counts as blocked (e.g. a
 * commit that waits for the marker of an earlier transaction, levels 0/1); the scenario goes on
 * and the statement completes later, as in Hermitage's "T2 BLOCKS" steps. Each row of the test
 * table is a data file of its own (rows are inserted one by one), as Hermitage assumes row-level
 * changes: LakeVilla detects write-write conflicts per data file.
 * <p>
 * Expectations (the anomaly must not be observable):
 * <ul>
 * <li>{@code ISOLATION=serializable} (Trino's SERIALIZABLE): all anomalies are prevented;</li>
 * <li>{@code ISOLATION=snapshot} (REPEATABLE READ): all except the write skews G2-item, G2 and
 * G2-two-edges, which snapshot isolation allows (reported, not checked).</li>
 * </ul>
 * The isolation is detected from the catalog. Run with {@code docker/hermitage.sh}, or set
 * {@code LAKEVILLA_TRINO_URL} (e.g. {@code jdbc:trino://localhost:8080}) and, optionally,
 * {@code LAKEVILLA_CATALOG} (default {@code lakevilla}); without the URL the test is skipped.
 */
@EnabledIfEnvironmentVariable(named = "LAKEVILLA_TRINO_URL", matches = ".+")
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestLakeVillaHermitage
{
    private static final long BLOCKED_AFTER_MILLIS = 2_000;
    private static final long TIMEOUT_SECONDS = 120;
    private static final List<List<Integer>> INITIAL = rows(1, 10, 2, 20);

    private final String url = System.getenv("LAKEVILLA_TRINO_URL");
    private final String catalog = Optional.ofNullable(System.getenv("LAKEVILLA_CATALOG")).orElse("lakevilla");
    private final String schema = "hermitage_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private final List<String> report = new ArrayList<>();
    private boolean serializable;

    @BeforeAll
    void setUp()
            throws SQLException
    {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        // the catalog supports SERIALIZABLE with ISOLATION=serializable, otherwise REPEATABLE READ
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("SHOW TABLES").close();
                serializable = true;
                connection.rollback();
            }
            catch (SQLException e) {
                if (e.getErrorCode() != UNSUPPORTED_ISOLATION_LEVEL.toErrorCode().getCode()) {
                    throw e;
                }
                serializable = false;
            }
        }
    }

    @AfterAll
    void tearDown()
            throws SQLException
    {
        // one line per print: the test log prefixes every write to stdout
        System.out.println("=== Hermitage, catalog %s, isolation %s, levels %s".formatted(catalog,
                serializable ? "serializable" : "snapshot", Optional.ofNullable(System.getenv("LAKEVILLA_LEVELS")).orElse("?")));
        report.stream().sorted().forEach(line -> System.out.println("=== " + line));
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    // G0: write cycles (dirty writes)
    @Test
    void g0()
            throws Exception
    {
        try (Scenario h = new Scenario("g0")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 12 WHERE id = 1");
            t1.run("UPDATE {t} SET value = 21 WHERE id = 2");
            Step c1 = t1.commit();
            t2.run("UPDATE {t} SET value = 22 WHERE id = 2");
            Step c2 = t2.commit();
            List<List<Integer>> table = h.table(c1, c2);
            // the rows are both of T1 or both of T2 (or unchanged)
            h.result(List.of(INITIAL, rows(1, 11, 2, 21), rows(1, 12, 2, 22)).contains(table), true, c1, c2);
        }
    }

    // G1a: aborted reads (dirty reads of aborted transactions)
    @Test
    void g1a()
            throws Exception
    {
        try (Scenario h = new Scenario("g1a")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("UPDATE {t} SET value = 101 WHERE id = 1");
            Step r1 = t2.run("SELECT * FROM {t}");
            Step c1 = t1.rollback();
            Step r2 = t2.run("SELECT * FROM {t}");
            Step c2 = t2.commit();
            h.table(c1, c2);
            h.result(r1.rows().equals(INITIAL) && r2.rows().equals(INITIAL), true, c1, c2);
        }
    }

    // G1b: intermediate reads (dirty reads of intermediate values)
    @Test
    void g1b()
            throws Exception
    {
        try (Scenario h = new Scenario("g1b")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("UPDATE {t} SET value = 101 WHERE id = 1");
            Step r1 = t2.run("SELECT * FROM {t}");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            Step c1 = t1.commit();
            Step r2 = t2.run("SELECT * FROM {t}");
            Step c2 = t2.commit();
            h.table(c1, c2);
            // T2 never sees 101; with snapshots it sees the rows before T1 in both reads
            h.result(r1.rows().equals(INITIAL) && r2.rows().equals(INITIAL), true, c1, c2);
        }
    }

    // G1c: circular information flow (dirty reads)
    @Test
    void g1c()
            throws Exception
    {
        try (Scenario h = new Scenario("g1c")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 22 WHERE id = 2");
            Step r1 = t1.run("SELECT * FROM {t} WHERE id = 2");
            Step r2 = t2.run("SELECT * FROM {t} WHERE id = 1");
            Step c1 = t1.commit();
            Step c2 = t2.commit();
            h.table(c1, c2);
            h.result(r1.rows().equals(rows(2, 20)) && r2.rows().equals(rows(1, 10)), true, c1, c2);
        }
    }

    // OTV: observed transaction vanishes
    @Test
    void otv()
            throws Exception
    {
        try (Scenario h = new Scenario("otv")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            Txn t3 = h.txn("T3");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            t1.run("UPDATE {t} SET value = 19 WHERE id = 2");
            t2.run("UPDATE {t} SET value = 12 WHERE id = 1");
            Step c1 = t1.commit();
            Step r1 = t3.run("SELECT * FROM {t} WHERE id = 1");
            t2.run("UPDATE {t} SET value = 18 WHERE id = 2");
            Step r2 = t3.run("SELECT * FROM {t} WHERE id = 2");
            Step c2 = t2.commit();
            Step c3 = t3.commit();
            h.table(c1, c2, c3);
            // T3 sees T1's rows (11, 19) and not a mix with T2's (12, 18)
            List<List<Integer>> seen = ImmutableList.<List<Integer>>builder().addAll(r1.rows()).addAll(r2.rows()).build();
            h.result(seen.equals(rows(1, 11, 2, 19)) || seen.equals(rows(1, 12, 2, 18)), true, c1, c2, c3);
        }
    }

    // PMP: predicate many preceders (phantoms)
    @Test
    void pmp()
            throws Exception
    {
        try (Scenario h = new Scenario("pmp")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            Step r1 = t1.run("SELECT * FROM {t} WHERE value = 30");
            t2.run("INSERT INTO {t} VALUES (3, 30)");
            Step c2 = t2.commit();
            Step r2 = t1.run("SELECT * FROM {t} WHERE value % 3 = 0");
            Step c1 = t1.commit();
            h.table(c1, c2);
            h.result(r1.rows().isEmpty() && r2.rows().isEmpty(), true, c1, c2);
        }
    }

    // PMP-write: predicate many preceders with a write
    @Test
    void pmpWrite()
            throws Exception
    {
        try (Scenario h = new Scenario("pmp_write")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("UPDATE {t} SET value = value + 10");
            t2.run("DELETE FROM {t} WHERE value = 20");
            Step c1 = t1.commit();
            t2.run("SELECT * FROM {t} WHERE value = 20");
            Step c2 = t2.commit();
            List<List<Integer>> table = h.table(c1, c2);
            // T2 aborted, or the result of a serial order: T1 then T2 (deletes id 1), T2 then T1 (deletes id 2)
            h.result(List.of(rows(1, 20, 2, 30), rows(2, 30), rows(1, 20)).contains(table)
                    && (c2.committed() || table.equals(rows(1, 20, 2, 30))), true, c1, c2);
        }
    }

    // P4: lost update
    @Test
    void p4()
            throws Exception
    {
        try (Scenario h = new Scenario("p4")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("SELECT * FROM {t} WHERE id = 1");
            t2.run("SELECT * FROM {t} WHERE id = 1");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 11 WHERE id = 1");
            Step c1 = t1.commit();
            Step c2 = t2.commit();
            h.table(c1, c2);
            // both read 10 and wrote 11: only one of them may commit
            h.result(!(c1.committed() && c2.committed()), true, c1, c2);
        }
    }

    // G-single: single anti-dependency cycles (read skew)
    @Test
    void gSingle()
            throws Exception
    {
        try (Scenario h = new Scenario("g_single")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            Step r1 = t1.run("SELECT * FROM {t} WHERE id = 1");
            t2.run("SELECT * FROM {t} WHERE id = 1");
            t2.run("SELECT * FROM {t} WHERE id = 2");
            t2.run("UPDATE {t} SET value = 12 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 18 WHERE id = 2");
            Step c2 = t2.commit();
            Step r2 = t1.run("SELECT * FROM {t} WHERE id = 2");
            Step c1 = t1.commit();
            h.table(c1, c2);
            h.result(r1.rows().equals(rows(1, 10)) && r2.rows().equals(rows(2, 20)), true, c1, c2);
        }
    }

    // G-single-write: read skew with a write of the stale value
    @Test
    void gSingleWrite()
            throws Exception
    {
        try (Scenario h = new Scenario("g_single_write")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("SELECT * FROM {t} WHERE id = 1");
            t2.run("SELECT * FROM {t}");
            t2.run("UPDATE {t} SET value = 12 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 18 WHERE id = 2");
            Step c2 = t2.commit();
            t1.run("DELETE FROM {t} WHERE value = 20");
            Step c1 = t1.commit();
            List<List<Integer>> table = h.table(c1, c2);
            // both committed and T1 deleted the row T2 had changed (from 20 to 18): no serial order
            h.result(!(c1.committed() && c2.committed() && table.equals(rows(1, 12))), true, c1, c2);
        }
    }

    // G2-item: item anti-dependency cycles (write skew on disjoint rows)
    @Test
    void g2Item()
            throws Exception
    {
        try (Scenario h = new Scenario("g2_item")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("SELECT * FROM {t} WHERE id IN (1, 2)");
            t2.run("SELECT * FROM {t} WHERE id IN (1, 2)");
            t1.run("UPDATE {t} SET value = 11 WHERE id = 1");
            t2.run("UPDATE {t} SET value = 21 WHERE id = 2");
            Step c1 = t1.commit();
            Step c2 = t2.commit();
            h.table(c1, c2);
            h.result(!(c1.committed() && c2.committed()), serializable, c1, c2);
        }
    }

    // G2: anti-dependency cycles (write skew on predicate reads)
    @Test
    void g2()
            throws Exception
    {
        try (Scenario h = new Scenario("g2")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            t1.run("SELECT * FROM {t} WHERE value % 3 = 0");
            t2.run("SELECT * FROM {t} WHERE value % 3 = 0");
            t1.run("INSERT INTO {t} VALUES (3, 30)");
            t2.run("INSERT INTO {t} VALUES (4, 42)");
            Step c1 = t1.commit();
            Step c2 = t2.commit();
            h.table(c1, c2);
            h.result(!(c1.committed() && c2.committed()), serializable, c1, c2);
        }
    }

    // G2-two-edges: anti-dependency cycle with a read-only transaction that observes it
    @Test
    void g2TwoEdges()
            throws Exception
    {
        try (Scenario h = new Scenario("g2_two_edges")) {
            Txn t1 = h.txn("T1");
            Txn t2 = h.txn("T2");
            Txn t3 = h.txn("T3");
            t1.run("SELECT * FROM {t}");
            t2.run("UPDATE {t} SET value = value + 5 WHERE id = 2");
            Step c2 = t2.commit();
            Step r3 = t3.run("SELECT * FROM {t}");
            Step c3 = t3.commit();
            t1.run("UPDATE {t} SET value = 0 WHERE id = 1");
            Step c1 = t1.commit();
            h.table(c1, c2, c3);
            // T3 saw T2's change, T1 committed after reading the state before T2: a cycle
            h.result(!(c1.committed() && c2.committed() && r3.rows().contains(List.of(2, 25))), serializable, c1, c2, c3);
        }
    }

    private Connection connect()
            throws SQLException
    {
        return DriverManager.getConnection("%s/%s/%s?user=hermitage".formatted(url, catalog, schema));
    }

    private static List<List<Integer>> rows(int... values)
    {
        ImmutableList.Builder<List<Integer>> rows = ImmutableList.builder();
        for (int i = 0; i < values.length; i += 2) {
            rows.add(List.of(values[i], values[i + 1]));
        }
        return rows.build();
    }

    private static boolean isConflict(Throwable e)
    {
        return e instanceof SQLException sqlException && sqlException.getErrorCode() == TRANSACTION_CONFLICT.toErrorCode().getCode();
    }

    /**
     * A Hermitage scenario on its own table test(id, value) with the rows (1, 10), (2, 20)
     */
    private final class Scenario
            implements AutoCloseable
    {
        private final String name;
        private final String table;
        private final List<Txn> transactions = new ArrayList<>();
        private final List<String> blocked = new ArrayList<>();
        private List<List<Integer>> finalRows;

        Scenario(String name)
                throws SQLException
        {
            this.name = name;
            this.table = schema + "." + name;
            try (Connection connection = connect(); Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + table + " (id INTEGER, value INTEGER)");
                // one data file per row
                statement.execute("INSERT INTO " + table + " VALUES (1, 10)");
                statement.execute("INSERT INTO " + table + " VALUES (2, 20)");
            }
        }

        Txn txn(String txnName)
                throws SQLException
        {
            Txn txn = new Txn(this, txnName);
            transactions.add(txn);
            return txn;
        }

        /**
         * The rows after all transactions ended
         */
        List<List<Integer>> table(Step... commits)
                throws Exception
        {
            for (Step commit : commits) {
                commit.await();
            }
            try (Connection connection = connect()) {
                finalRows = query(connection, "SELECT * FROM " + table);
            }
            return finalRows;
        }

        void result(boolean prevented, boolean mustBePrevented, Step... commits)
        {
            StringBuilder line = new StringBuilder("%-15s %-10s".formatted(name, prevented ? "prevented" : "ANOMALY"));
            for (Step commit : commits) {
                String outcome = commit.sql.equals("ROLLBACK") ? "rolled back" : "committed";
                line.append(" %s %s".formatted(commit.txn.name, commit.committed() ? outcome : "aborted"));
            }
            if (!blocked.isEmpty()) {
                line.append(", blocked: ").append(String.join(", ", blocked));
            }
            line.append(", rows ").append(finalRows);
            report.add(line.toString());
            if (mustBePrevented) {
                assertThat(prevented).describedAs(line.toString()).isTrue();
            }
        }

        @Override
        public void close()
        {
            transactions.forEach(Txn::close);
        }
    }

    /**
     * A Hermitage transaction: a JDBC connection with its own thread
     */
    private final class Txn
    {
        private final Scenario scenario;
        private final String name;
        private final Connection connection;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        Txn(Scenario scenario, String name)
                throws SQLException
        {
            this.scenario = scenario;
            this.name = name;
            this.connection = connect();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(serializable ? Connection.TRANSACTION_SERIALIZABLE : Connection.TRANSACTION_REPEATABLE_READ);
        }

        Step run(String sql)
                throws Exception
        {
            String statement = sql.replace("{t}", scenario.table);
            return submit(statement, () -> query(connection, statement));
        }

        Step commit()
                throws Exception
        {
            return submit("COMMIT", () -> {
                connection.commit();
                return List.of();
            });
        }

        Step rollback()
                throws Exception
        {
            return submit("ROLLBACK", () -> {
                connection.rollback();
                return List.of();
            });
        }

        // runs the statement in the transaction's thread; it counts as blocked if it does not
        // finish within BLOCKED_AFTER_MILLIS
        private Step submit(String sql, Callable<List<List<Integer>>> action)
                throws Exception
        {
            Step step = new Step(this, sql, executor.submit(action));
            try {
                step.future.get(BLOCKED_AFTER_MILLIS, MILLISECONDS);
            }
            catch (TimeoutException e) {
                scenario.blocked.add(name + " " + sql.split(" ")[0]);
            }
            catch (ExecutionException e) {
                // failures are reported by the step's result
            }
            return step;
        }

        void close()
        {
            executor.shutdownNow();
            try {
                connection.close();
            }
            catch (SQLException e) {
                // the transaction may have failed
            }
        }
    }

    /**
     * A statement of a transaction: rows of a query, or the outcome of a commit
     */
    private static final class Step
    {
        private final Txn txn;
        private final String sql;
        private final Future<List<List<Integer>>> future;

        Step(Txn txn, String sql, Future<List<List<Integer>>> future)
        {
            this.txn = txn;
            this.sql = sql;
            this.future = future;
        }

        void await()
                throws Exception
        {
            try {
                future.get(TIMEOUT_SECONDS, SECONDS);
            }
            catch (ExecutionException e) {
                if (!isConflict(e.getCause())) {
                    throw new AssertionError("%s: %s failed".formatted(txn.name, sql), e.getCause());
                }
            }
        }

        List<List<Integer>> rows()
                throws Exception
        {
            try {
                return future.get(TIMEOUT_SECONDS, SECONDS);
            }
            catch (ExecutionException e) {
                throw new AssertionError("%s: %s failed".formatted(txn.name, sql), e.getCause());
            }
        }

        /**
         * The statement succeeded; false if LakeVilla aborted the transaction (conflict)
         */
        boolean committed()
        {
            try {
                future.get(TIMEOUT_SECONDS, SECONDS);
                return true;
            }
            catch (ExecutionException e) {
                if (isConflict(e.getCause())) {
                    return false;
                }
                throw new AssertionError("%s: %s failed".formatted(txn.name, sql), e.getCause());
            }
            catch (InterruptedException | TimeoutException e) {
                throw new AssertionError("%s: %s did not finish".formatted(txn.name, sql), e);
            }
        }
    }

    // rows (id, value) of a query, ordered; the row count of other statements
    private static List<List<Integer>> query(Connection connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            if (!statement.execute(sql)) {
                return List.of();
            }
            List<List<Integer>> rows = new ArrayList<>();
            try (ResultSet resultSet = statement.getResultSet()) {
                int columns = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    List<Integer> row = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        row.add(resultSet.getInt(i));
                    }
                    rows.add(List.copyOf(row));
                }
            }
            rows.sort(Comparator.<List<Integer>, Integer>comparing(row -> row.get(0)).thenComparing(row -> row.size() > 1 ? row.get(1) : 0));
            return List.copyOf(rows);
        }
    }
}
