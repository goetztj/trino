# Trino LakeVilla connector

The LakeVilla connector gives Trino SQL access to Delta Lake tables through
[LakeVilla](https://github.com/goetztj/LakeVilla), a client-side library that
adds multi-statement, multi-table ACID transactions to open table formats while
keeping the tables readable and writable by other engines (Spark, Trino's own
Delta Lake connector, ...).

Every Trino transaction is one LakeVilla transaction:

```sql
START TRANSACTION;
UPDATE bank.accounts SET balance = balance - 10 WHERE id = 1;
UPDATE bank.accounts SET balance = balance + 10 WHERE id = 2;
INSERT INTO bank.transfers VALUES (1, 2, 10);
COMMIT;   -- both tables change atomically, or the transaction aborts
```

Contents:

1. [Architecture](#1-architecture)
2. [Getting started: build, run, use and test](#2-getting-started-build-run-use-and-test)
3. [Building](#3-building)
4. [Installing into a Trino server](#4-installing-into-a-trino-server)
5. [Configuration](#5-configuration)
6. [Tables, schemas and types](#6-tables-schemas-and-types)
7. [SQL support](#7-sql-support)
8. [Transactions](#8-transactions)
9. [Testing](#9-testing)
10. [Limitations](#10-limitations)
11. [Troubleshooting](#11-troubleshooting)
12. [Caching](#12-caching)

## 1. Architecture

```
 Trino (single node: coordinator executes the tasks)
 ┌────────────────────────────────────────────────────────────────────────┐
 │ LakeVillaConnector                                                     │
 │   LakeVillaMetadata ── LakeVillaStore ────────────┐  schemas, tables,  │
 │   LakeVillaSplitManager                           │  CREATE/DROP       │
 │   LakeVillaPageSource ┐                           │                    │
 │   LakeVillaPageSink   ├─ LakeVillaTransaction ──┐ │  one per Trino     │
 │                       │  (LakeVillaTransaction- │ │  transaction       │
 │                       │   Manager: UUID -> txn) │ │                    │
 │            Arrow IPC  │                         │ │                    │
 │ ─────────────────── JNI: LakeVillaNative ─────────────────────────────  │
 │ liblakevilla.so (jni_trino.cpp)                 │ │                    │
 │   TransactionManagerDeltaLake  <────────────────┘ │                    │
 │   MinIOConnector (AWS SDK)     <──────────────────┘                    │
 └────────────────────────────────────────────────────────────────────────┘
                                   │ S3 API
                           object store (MinIO/S3): Delta tables
```

- The Java side (`plugin/trino-lakevilla`) is a regular Trino plugin. It calls
  the native LakeVilla library `liblakevilla.so` through JNI
  (`LakeVillaNative`); rows are exchanged as Arrow IPC streams.
- The native side is part of the LakeVilla repository:
  `LV/src/LakeVilla/TransactionManager/jni_trino.cpp` (the binding),
  built into `liblakevilla.so` together with the YCSB-LH binding.
- A Trino transaction begins a LakeVilla transaction (`TransactionManagerDeltaLake`)
  with the first table it reads or writes; further tables join the transaction
  when they are accessed. `COMMIT` commits it (or reports the conflict that
  aborted it), `ROLLBACK` aborts it.
- The LakeVilla transaction lives in the memory of the Trino node that began it.
  Reads and writes therefore run on that node: the connector needs a Trino
  deployment in which the coordinator executes tasks (a single-node server,
  `node-scheduler.include-coordinator=true`).
- There is no catalog service: schemas and tables are prefixes in the bucket,
  found by listing it (Spark's warehouse layout, see [section 6](#6-tables-schemas-and-types)).

## 2. Getting started: build, run, use and test

`docker/` contains a complete environment: MinIO, and a Trino server with the
LakeVilla connector (catalog `lakevilla`), the TPC-H connector (`tpch`, for
test data) and Trino's Delta Lake connector (`delta`, to check
interoperability). To install the connector into another Trino server, see
[section 4](#4-installing-into-a-trino-server).

### 2.1 Prerequisites

- Linux x86_64, Docker with compose v2 and buildx, Python 3 (for the SQL
  tests), network access for the first build, about 10 GB of disk space. No
  local JDK or C++ toolchain is needed: Maven and the C++ build run in
  containers.
- The LakeVilla repository next to this repository (as in the LakeVilla
  checkout layout); otherwise set `LAKEVILLA_REPO` to its path:

  ```
  LakeVilla/
  ├── LakeVilla/   # LakeVilla repository (LAKEVILLA_REPO)
  └── trino/           # this repository
  ```

### 2.2 Build

```bash
cd plugin/trino-lakevilla/docker
./build.sh                     # the first run takes 15-30 minutes (AWS SDK, Trino)
```

`build.sh` (`--skip-maven` reuses the last Maven build, e.g. after changes in
LakeVilla only; `--skip-images` skips the images):

1. builds Trino (`trino-server-core`, the plugins `lakevilla`, `tpch` and
   `delta-lake`, the CLI and, for the tests, the JDBC driver) with `./mvnw` in
   an `eclipse-temurin:24-jdk` container, using your `~/.m2` as Maven
   repository,
2. copies the artifacts to `docker/dist/` (not versioned),
3. builds the images: `lakevilla/trino:local` compiles `liblakevilla.so` from
   `$LAKEVILLA_REPO/LV/src` (Ubuntu 24.04, JDK 24, Arrow 24, aws-sdk-cpp
   1.11.880, installed with LakeVilla's `setup_ubuntu.sh`); MinIO is built from
   `$LAKEVILLA_REPO/MinIO` as in the LakeVilla repository (existing images are
   reused).

### 2.3 Run

```bash
docker compose up -d           # MinIO + Trino on http://localhost:8080 (TRINO_PORT for another port)
docker compose ps              # trino is "healthy" after about 20 seconds
docker compose logs -f trino   # server log (includes LakeVilla's messages)
```

The configuration in `docker/etc/` is mounted into the container: edit it and
restart Trino (`docker compose restart trino`). Environment variables of
`docker compose up`:

| Variable | Default | |
|---|---|---|
| `LAKEVILLA_LEVELS` | `01` | LakeVilla's levels (`lakevilla.levels`), e.g. `LAKEVILLA_LEVELS=012 docker compose up -d trino` |
| `LAKEVILLA_CONFIG` | `lvconfig.conf` | LakeVilla configuration in `etc/lakevilla/` (`lvconfig-snapshot.conf`: `ISOLATION=snapshot`) |
| `TRINO_PORT` | `8080` | host port of Trino |
| `LV_TRINO_PROJECT`, `LV_TRINO_TAG` | `lv-trino`, `local` | compose project and image tag (`lakevilla/trino:<tag>`), to run several stacks, e.g. of another branch, side by side (also for `build.sh`) |

The stack publishes only Trino's port and has no fixed container names, so it
runs next to the containers of the LakeVilla repository.

### 2.4 Use

With the Trino CLI in the container, or any Trino client (JDBC URL
`jdbc:trino://localhost:8080/lakevilla?user=<name>`):

```bash
docker compose exec trino trino --catalog lakevilla
```

```sql
CREATE SCHEMA lakevilla.demo;
USE lakevilla.demo;

CREATE TABLE accounts (id BIGINT NOT NULL, owner VARCHAR, balance DECIMAL(12, 2));
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50);

START TRANSACTION;                                          -- one LakeVilla transaction
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
UPDATE accounts SET balance = balance + 10 WHERE id = 2;
COMMIT;                                                     -- or ROLLBACK

CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders;      -- test data of the tpch catalog
SELECT orderpriority, count(*) FROM orders GROUP BY 1;
```

With JDBC, `setAutoCommit(false)` starts a transaction (`setTransactionIsolation`
chooses the level) and `commit()`/`rollback()` end it. A commit that conflicts
with a concurrent transaction fails with `TRANSACTION_CONFLICT`: retry the
transaction. See [SQL support](#7-sql-support) and [Transactions](#8-transactions).

The tables are Delta tables in the bucket `warehouse` of MinIO
(`s3://warehouse/wh/demo.db/accounts/`):

```bash
docker compose exec mc mc ls -r minio/warehouse/wh/
```

### 2.5 Test

All tests are described in [section 9](#9-testing). With the stack running
(in `docker/`):

```bash
python3 test_lakevilla.py                     # SQL end-to-end tests, about 1 minute
python3 test_lakevilla.py --levels 01,2,012   # restarts Trino for each level combination
python3 test_lakevilla.py -k update           # only tests whose name contains "update"
./hermitage.sh                                # Hermitage: isolation anomalies over JDBC
```

**Hermitage** (`TestLakeVillaHermitage`, [section 9.1](#91-hermitage)) runs
the isolation anomalies of [Hermitage](https://github.com/ept/hermitage) as
concurrent SQL transactions over JDBC. `hermitage.sh` restarts Trino for every
combination of isolation and levels and runs the test with Maven in a JDK
container in the stack's network:

```bash
./hermitage.sh                                                    # serializable and snapshot, levels 01,2,012
./hermitage.sh --isolation serializable --levels 01,1,2,012,none  # other combinations
```

It prints one line per scenario and `passed` or `FAILED` per combination
(the exit code is 1 if a combination failed), and leaves Trino running with the
default configuration:

```
--- isolation serializable, levels 01
Hermitage, catalog lakevilla, isolation serializable, levels 01
g0              prevented  T1 committed T2 aborted, rows [[1, 11], [2, 21]]
...
g_single        prevented  T1 committed T2 committed, blocked: T2 COMMIT, rows [[1, 12], [2, 18]]
...
[INFO] Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
passed
```

`prevented`: the anomaly was not observable; `ANOMALY`: it was (allowed only
for the write skews G2-item and G2 under `ISOLATION=snapshot`, otherwise the
test fails); `T1 committed`/`aborted`: outcome of the transaction's commit;
`blocked`: the statement waited for another transaction (markers); `rows`: the
table afterwards. To run the test against another Trino server with a
LakeVilla catalog (needs a local JDK 24; in the repository root):

```bash
LAKEVILLA_TRINO_URL=jdbc:trino://<host>:8080 LAKEVILLA_CATALOG=lakevilla LAKEVILLA_LEVELS=01 \
  ./mvnw -pl plugin/trino-lakevilla test -Dtest=TestLakeVillaHermitage -Dsurefire.failIfNoSpecifiedTests=false
```

The isolation is detected from the catalog; `LAKEVILLA_LEVELS` only labels the
report. Without `LAKEVILLA_TRINO_URL` the test is skipped. The unit tests need
no server: `./mvnw -pl plugin/trino-lakevilla test` (local JDK 24, after `build.sh`).

### 2.6 Stop

```bash
docker compose down            # keeps the data (volume minio-data)
docker compose down -v         # deletes the data as well
```

## 3. Building

### 3.1 The native library `liblakevilla.so`

`liblakevilla.so` is built from the LakeVilla repository with the JNI option
(default `ON`; CMake needs a JDK for the JNI headers):

```bash
# dependencies: LV/setup_ubuntu.sh (Arrow 24, aws-sdk-cpp 1.11.880 in /LakeVilla/aws)
git submodule update --init LV/src/rapidjson
cmake -S LV/src -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DAWSSDK_ROOT_DIR=/LakeVilla/aws -DLV_BUILD_JNI=ON \
      -DLV_BUILD_BENCHMARKS=OFF -DLV_BUILD_COMPAT=OFF
cmake --build build --target lakevilla
nm -D --defined-only build/liblakevilla.so | grep Java_io_trino_plugin_lakevilla   # the Trino binding
```

Or in Docker, without local dependencies (writes `liblakevilla.so` to `<dir>`):

```bash
cd plugin/trino-lakevilla/docker
docker build --build-context lakevilla=<LakeVilla repository>/LV --target lv-lib --output type=local,dest=<dir> .
```

The library links dynamically against Arrow/Parquet 24 and the AWS SDK; the
machine running Trino needs these libraries (e.g. `LD_LIBRARY_PATH=/LakeVilla/aws/lib`).
The prebuilt `LV/binaries/*/liblakevilla.so` of the LakeVilla repository
predate the Trino binding: build the library from source.

### 3.2 The plugin

The plugin is a module of this repository (JDK 24, Maven wrapper):

```bash
./mvnw -pl plugin/trino-lakevilla -am install -DskipTests      # plugin and its dependencies
./mvnw -pl plugin/trino-lakevilla verify                        # with the unit tests and Trino's code checks
```

The plugin is `plugin/trino-lakevilla/target/trino-lakevilla-<version>.zip`.
Without a local JDK, run Maven in a container as `docker/build.sh` does.

## 4. Installing into a Trino server

The plugin checks that the server has the same SPI version
(`checkStrictSpiVersionMatch`): use a server built from this repository
(`core/trino-server-core` or `core/trino-server`, same version).

1. Unpack the plugin zip into `<trino>/plugin/lakevilla/` and copy
   `liblakevilla.so` into the same directory (or set
   `lakevilla.native-library`, or put it on `java.library.path`).
2. Make Arrow/Parquet 24 and the AWS SDK loadable for the dynamic linker
   (system packages, `LD_LIBRARY_PATH`, or `ldconfig`).
3. Add to `etc/jvm.config` (Arrow's memory management and JNI):
   ```
   --add-opens=java.base/java.nio=ALL-UNNAMED
   --sun-misc-unsafe-memory-access=allow
   --enable-native-access=ALL-UNNAMED
   ```
4. Run Trino as a single node, or let the coordinator execute tasks
   (`etc/config.properties`):
   ```
   coordinator=true
   node-scheduler.include-coordinator=true
   ```
5. Create the LakeVilla configuration and the catalog
   (`etc/catalog/lakevilla.properties`), see [section 5](#5-configuration).

`docker/Dockerfile` (target `trino`) and `docker/etc/` are a complete example.

## 5. Configuration

### 5.1 Catalog properties

`etc/catalog/lakevilla.properties`:

```properties
connector.name=lakevilla
lakevilla.config-path=/etc/trino/lakevilla/lvconfig.conf
lakevilla.levels=01
lakevilla.warehouse=wh/
```

| Property | Default | Description |
|---|---|---|
| `lakevilla.config-path` | (required) | LakeVilla configuration file, see 5.2. |
| `lakevilla.levels` | `01` | LakeVilla's protocol levels: `0` markers and sublogs, `1` markers and dependency tracking, `2` global version log. `01` = LV[Write], `2` = LV[Read], `012` = LV[Hybrid], `none`. See [section 8](#8-transactions). |
| `lakevilla.warehouse` | `wh/` | Prefix of the warehouse in the bucket (Spark's `spark.sql.warehouse.dir`), see [section 6](#6-tables-schemas-and-types). |
| `lakevilla.redo` | `false` | Level 0: redo the operations of a conflicting transaction against the newest snapshot instead of aborting it (LakeVilla's `REDO`). Only correct if the transactions' writes do not depend on their reads, see [section 8](#8-transactions). `UPDATE`, `MERGE` and row-level `DELETE` need `false`. |
| `lakevilla.max-rows-per-file` | `1000000` | Maximum rows of a data file written by `INSERT`/`CREATE TABLE AS`. |
| `lakevilla.native-library` | | Path of `liblakevilla.so`; default: the plugin directory, then `java.library.path`. |

### 5.2 LakeVilla configuration (`lvconfig.conf`)

The connector reads the object store and protocol settings from a LakeVilla
configuration file (format and keys: README of the LakeVilla repository and
`LV/README.md`). Relevant keys:

```properties
HOST=minio:9000          # S3 endpoint (http)
USER=admin
PASSWD=password
AWSREGION=us-east-1
BUCKET=warehouse         # bucket of the warehouse
PathToGlobalLog=wh/_global/log.txt         # level 2
PathToGlobalTmpLog=wh/_global/log.txt.tmp
#ISOLATION=serializable  # or snapshot
#MARKER_LEASE_MS=30000   # markers (levels 0/1): a crashed client blocks writers at most this long
#MAX_REDOS=3
#CHECKPOINTS=1           # Delta checkpoints after commits, like Spark
# level 2 and other engines writing the same tables (Spark, Trino's Delta connector):
GLOBAL_LOG_CHECK_AT_BEGIN=1   # their commits join the snapshot
GLOBAL_LOG_REPAIR=1           # LakeVilla commits after their commits
```

`REDO` of the file is ignored: the connector sets it from `lakevilla.redo`.
All Trino nodes and all other LakeVilla clients of the same tables must use the
same global log paths.

## 6. Tables, schemas and types

Schemas and tables follow the layout of Spark's warehouse directory, so tables
created by Spark (`spark.sql.warehouse.dir=s3a://<bucket>/<warehouse>`) appear
in Trino and vice versa:

| Trino | Object store |
|---|---|
| table `default.t` | `s3://<BUCKET>/<warehouse>t/` |
| schema `s` | `s3://<BUCKET>/<warehouse>s.db/` |
| table `s.t` | `s3://<BUCKET>/<warehouse>s.db/t/` |

A table is a prefix with a Delta log (`_delta_log/`). `SHOW SCHEMAS`/`SHOW
TABLES` list the bucket (`CREATE SCHEMA` writes an empty directory marker
`s.db/`). Names with upper-case letters cannot be addressed by Trino and are
not listed.

Column types (LakeVilla describes a table by the Arrow schema of its Delta
schema):

| Delta | Trino |
|---|---|
| `boolean` | `boolean` |
| `byte`, `short`, `integer`, `long` | `tinyint`, `smallint`, `integer`, `bigint` |
| `float`, `double` | `real`, `double` |
| `decimal(p,s)` | `decimal(p,s)` |
| `string` | `varchar` |
| `binary` | `varbinary` |
| `date` | `date` |
| `timestamp` | `timestamp(6) with time zone` (UTC) |
| `timestamp_ntz` | `timestamp(6)` |

`CREATE TABLE` maps `varchar(n)` and `char(n)` to `varchar`, other timestamp
precisions to microseconds. Columns of other Delta types (`array`, `map`,
`struct`) are not shown; Trino cannot create them. Column comments, table
comments and `NOT NULL` are written to the Delta schema.

## 7. SQL support

| Statement | Support |
|---|---|
| `SELECT` | All supported types. The snapshot of a table is read at once (one split per table). |
| `INSERT`, `INSERT ... SELECT` | New data files of the transaction. Missing nullable columns are null. |
| `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS` | Delta table (protocol of Spark 3.5 / Delta 3.0), comments, `NOT NULL`. No table properties, partitioning or `OR REPLACE`. |
| `CREATE TABLE AS` | The table is created first, the rows are written in the transaction; the table is dropped again if the transaction does not commit. |
| `UPDATE` | Any `SET` expressions and conditions, including the first (key) column. Row-level change, see [7.1](#71-row-level-changes). |
| `MERGE` | All clauses (`WHEN MATCHED [AND ...] THEN UPDATE/DELETE`, `WHEN NOT MATCHED THEN INSERT`). Row-level change. |
| `DELETE FROM t WHERE <condition>` | Any condition, also with subqueries. Row-level change. |
| `DELETE FROM t WHERE <first column> = v`, `IN (...)`, `BETWEEN` (integers, dates) | Fast path: LakeVilla's key-based removal without a scan in Trino (the key column is the first column; up to 1000 keys of type `tinyint`/`smallint`/`integer`/`bigint`/`date`/`varchar`). Other rows of the affected files are kept. |
| `DELETE FROM t`, `TRUNCATE TABLE t` | Removes all data files of the snapshot. |
| `DROP TABLE` | Deletes all objects of the table (like Spark's `DROP TABLE` of a managed table). |
| `CREATE SCHEMA`, `DROP SCHEMA [CASCADE]` | Directory marker; `CASCADE` drops the tables. |
| `SHOW SCHEMAS/TABLES/COLUMNS`, `DESCRIBE`, `SHOW CREATE TABLE`, `information_schema` | Yes. |
| `START TRANSACTION`, `COMMIT`, `ROLLBACK` | Yes, including writes to several tables (see below). |
| `ALTER TABLE`, views, time travel (`FOR VERSION AS OF`), table statistics | No. |

### 7.1 Row-level changes

`UPDATE`, `MERGE` and `DELETE` with conditions other than the key change rows
copy-on-write, like Spark's `DELETE`/`UPDATE` on Delta tables without deletion
vectors:

1. **Scan**: the table is read data file by data file (in the transaction's
   snapshot, including its own changes). Every row gets a row id
   `row(file varchar, position bigint)`: its data file and its position in
   the file (`LakeVillaPageSource`, LakeVilla's `list_data_files` and
   `read_snapshot_file`).
2. **Merge sink** (`LakeVillaMergeSink`): Trino deletes the old version of a
   changed row and inserts the new one (`DELETE_ROW_AND_INSERT_ROW`). Inserted
   rows are written as new data files of the transaction, like `INSERT`. The
   deleted row ids are collected per data file and passed to the coordinator.
3. **Finish** (`LakeVillaMetadata.finishMerge`): every affected data file is
   rewritten once, without its deleted rows: LakeVilla's `remove_rows` writes
   the remaining rows to a new data file and removes the old one.

All of it is part of the LakeVilla transaction: nothing is visible before the
commit, and a concurrent commit that removed one of the rewritten files makes
the transaction abort (`TRANSACTION_CONFLICT`). Conflicts are detected per
data file, as in Delta: two transactions that change different rows of the
same file conflict. Row-level changes need `lakevilla.redo=false` (the
default): a redo against a newer snapshot cannot repeat the removal of rows of
a file that is gone in it.

## 8. Transactions

- **Scope**: all statements of a Trino transaction (`START TRANSACTION` ...
  `COMMIT`, or one statement in auto-commit mode) form one LakeVilla
  transaction. A table joins it when it is first read or written; its snapshot
  is taken then. The transaction reads its own writes.
- **Commit**: the changes of all tables are committed by LakeVilla. If
  LakeVilla aborts the transaction because of a conflict, `COMMIT` (or the
  auto-commit statement) fails with `TRANSACTION_CONFLICT`; nothing of the
  transaction is visible and the application can retry it.
- **Isolation**: `ISOLATION=serializable` (default) aborts a transaction if a
  table it read was changed by a concurrent commit (table granularity), so
  lost updates and write skew are prevented; Trino's isolation level
  `SERIALIZABLE` is supported. With `ISOLATION=snapshot` only write-write
  conflicts abort (snapshot isolation, Trino's `REPEATABLE READ`).
- **Levels** (`lakevilla.levels`, the variants of the paper):
  - `01` (LV[Write], default) and `1`: markers reserve the next version of a
    table when the transaction opens it; concurrent writers of the table wait
    at their commit until the marker's transaction ends (at most
    `MARKER_LEASE_MS` for crashed clients). A long-running transaction that
    only reads a table therefore delays writers of that table.
  - `2` (LV[Read]): global version log; commits claim versions without
    markers. `012` (LV[Hybrid]) combines both.
  - `none`: plain optimistic commits with LakeVilla's validation.
- **Redo** (level 0, `lakevilla.redo`): LakeVilla can redo the logged operations
  of a conflicting transaction against the newest snapshot instead of aborting
  it. A redo does not recompute values the transaction read and used otherwise
  (e.g. `INSERT ... SELECT`, or an update computed from an earlier `SELECT`), so
  it breaks serializability for SQL. The connector disables redo by default;
  with `lakevilla.redo=true` it is only correct for blind writes.
- **DDL is not transactional**: `CREATE TABLE`, `DROP TABLE`, `CREATE/DROP
  SCHEMA` take effect immediately (also inside `START TRANSACTION`).
  `CREATE TABLE AS` drops its table if the transaction does not commit.
- **Other engines**: tables stay plain Delta tables. Spark and Trino's Delta
  Lake connector read LakeVilla's commits, and LakeVilla reads theirs. With
  level 2, LakeVilla only sees commits of other engines with
  `GLOBAL_LOG_CHECK_AT_BEGIN=1` and only commits after them with
  `GLOBAL_LOG_REPAIR=1` (set in `docker/etc/lakevilla/lvconfig.conf`). See the
  LakeVilla README for the known limitations (I11/I12: markers with concurrent
  Spark checkpoints; deletion vectors).

## 9. Testing

| Test | What | How |
|---|---|---|
| Unit tests (`src/test`) | Configuration, paths of schemas/tables, type mapping, Arrow conversion, key extraction (no native library needed) | `./mvnw -pl plugin/trino-lakevilla test` |
| End-to-end (`docker/test_lakevilla.py`) | SQL against the Docker stack: DDL, all types, `NOT NULL`, CTAS (also failed), `INSERT`, key/row-level/all `DELETE`, `UPDATE` (by key, by other columns, all rows, the key column, all types kept by the rewrite), `MERGE`, `DELETE` with a subquery, row-level changes in transactions (also of the transaction's own files), concurrent updates (lost update, same file, different tables), `TRUNCATE`, multi-table transactions with commit/rollback, repeatable reads with a concurrent writer, write-write conflicts (lost update), write skew, `DROP SCHEMA CASCADE`, Delta Lake interoperability with Trino's Delta Lake connector in both directions (including `UPDATE` by each of them) | `python3 docker/test_lakevilla.py` (running stack), `--levels 01,1,2,012,none` (restarts Trino per level combination), `-k <name>` |
| Hermitage (`src/test/.../TestLakeVillaHermitage.java`) | The isolation anomalies of [Hermitage](https://github.com/ept/hermitage) as concurrent SQL transactions over JDBC (see 9.1) | `docker/hermitage.sh` (running stack) |
| LakeVilla | Protocol, schema and key tests of the library, including the Trino-specific additions (`KeyRows.RedoOfARemovalWithoutUpdate`, `KeyRows.ConflictAbortsWithoutRedo`, `KeyRows.RemoveRows*`, `KeyRows.ConcurrentRemovalsOfOneFileConflict`) | LakeVilla repository: `ctest --test-dir build -L unit` (see its README) |

`test_lakevilla.py` needs only Python 3 (standard library). Every test runs in a
new schema that is dropped afterwards.

### 9.1 Hermitage

`TestLakeVillaHermitage` runs the scenarios of Martin Kleppmann's
[Hermitage](https://github.com/ept/hermitage) (PostgreSQL scripts) against the
running server: G0, G1a, G1b, G1c, OTV, PMP, PMP-write, P4, G-single,
G-single-write, G2-item, G2 and G2-two-edges. Every Hermitage transaction is a
JDBC connection (auto-commit off, `SERIALIZABLE` or `REPEATABLE READ`) with its
own thread; a statement that does not finish within 2 seconds counts as blocked
and the scenario continues (with markers, a commit waits for the transactions
that opened the table earlier). The rows of the test table are separate data
files, as Hermitage assumes row-level changes. A scenario checks that its
anomaly is not observable (and reports which transactions committed or
aborted and which statements blocked):

- `ISOLATION=serializable`: all anomalies must be prevented;
- `ISOLATION=snapshot`: all except the write skews G2-item, G2 and
  G2-two-edges, which snapshot isolation allows (reported only).

The test is skipped unless `LAKEVILLA_TRINO_URL` is set (e.g.
`jdbc:trino://localhost:8080`; `LAKEVILLA_CATALOG`, default `lakevilla`).
`docker/hermitage.sh` restarts Trino for every combination of isolation
(`etc/lakevilla/lvconfig.conf` or `lvconfig-snapshot.conf`, selected with
`LAKEVILLA_CONFIG`) and levels and runs the test with Maven in a JDK container
in the stack's network:

```bash
./hermitage.sh                                              # serializable and snapshot, levels 01, 2, 012
./hermitage.sh --isolation serializable --levels 01,1,2,012,none
```

Results (all combinations pass):

| Scenario | serializable, levels 01/1/012 | serializable, levels 2/none | snapshot, levels 01/1/012 | snapshot, levels 2/none |
|---|---|---|---|---|
| G0, G1c, OTV, P4, PMP-write | T2 aborts | T2 aborts | T2 aborts (G1c: both commit, each read the old value) | T2 aborts |
| G1a, G1b, G-single, PMP | prevented (G-single, PMP: T2's commit waits for T1) | prevented | prevented | prevented |
| G-single-write | T2 waits, then aborts | T1 aborts | T2 waits, then aborts | T1 aborts |
| G2-item, G2 | T2 aborts | T2 aborts | **anomaly** (allowed) | T2 aborts |
| G2-two-edges | T2 waits, then aborts | T1 aborts | prevented (T3 does not see T2's change) | T1 aborts |

Without markers (levels 2, none) a commit claims the next version of the table,
so a concurrent commit to the same table aborts the later transaction even
under `ISOLATION=snapshot` (stricter than snapshot isolation).

## 10. Limitations

- **Single node**: LakeVilla transactions live in the memory of the node that
  began them; reads and writes must run there (coordinator with
  `node-scheduler.include-coordinator=true`). Splits are bound to that node;
  writers on other workers fail with `LAKEVILLA_TRANSACTION_NOT_FOUND`.
- **Several catalogs** (e.g. one per level combination) share the native
  library; this works with static catalog management (`etc/catalog/`,
  checked with two catalogs). A JNI library can be loaded by one class
  loader only: if a catalog gets another class loader (e.g. created with
  `CREATE CATALOG` under dynamic catalog management), it fails with
  `liblakevilla.so is already loaded by another class loader`. All clients of a
  table should use the same levels.
- **Reads** load the whole snapshot of a table into memory (one split, no
  projection or predicate pushdown into LakeVilla, at most 2 GB per table as
  Arrow IPC stream). Key predicates (`WHERE <first column> IN (...)`) are
  filtered in Trino.
- **Writes**: no partitioned tables, no column mapping. Row-level changes
  rewrite whole data files (copy-on-write, no deletion vectors); each
  rewritten file is read twice (scan and rewrite). Tables with deletion
  vectors are not read correctly by LakeVilla (rows deleted by a vector
  remain, see the LakeVilla README).
- **Listing**: `SHOW SCHEMAS`/`SHOW TABLES` list all objects below the
  warehouse prefix; large warehouses list slowly.
- **Query retries** (`retry-policy`) are not supported.

## 11. Troubleshooting

| Symptom | Cause |
|---|---|
| `Unable to load liblakevilla.so` | The library is not in the plugin directory, or Arrow/Parquet 24 or the AWS SDK are not found by the dynamic linker (`ldd liblakevilla.so`). |
| `UnsatisfiedLinkError: ...LakeVillaNative.openStore` | `liblakevilla.so` was built without the Trino binding (`jni_trino.cpp`), e.g. the prebuilt library of `LV/binaries`. |
| `Failed to initialize MemoryUtil` / `java.nio` access errors | `--add-opens=java.base/java.nio=ALL-UNNAMED` is missing in `jvm.config`. |
| `LAKEVILLA_TRANSACTION_NOT_FOUND` | Multi-node cluster; see [Limitations](#10-limitations). |
| `TRANSACTION_CONFLICT` | LakeVilla aborted the transaction because of a concurrent commit: retry it. |
| Writers hang for up to 30 s | With markers (levels 0/1) a writer waits for the markers of earlier transactions on the table (e.g. an open `START TRANSACTION` that read the table, or a crashed client: `MARKER_LEASE_MS`). |
| `unable to begin a LakeVilla transaction` | The table's Delta log cannot be read (see the server log, LakeVilla prints its errors to stderr). |
| Unexpected rows after concurrent updates with `lakevilla.redo=true` | Redo re-applies writes without re-evaluating reads; use the default `lakevilla.redo=false`. |
| `UPDATE, MERGE and DELETE with conditions other than the key need lakevilla.redo=false` | Row-level changes are not redoable; see [7.1](#71-row-level-changes). |

## 12. Caching

The connector caches nothing across transactions. Within a transaction, a
table's schema is loaded once (`LakeVillaMetadata`) and its snapshot is taken
once (`LakeVillaTransaction`), but every scan reads all data files of the
snapshot again.

`LakeVillaCache` is the hook for caching. The connector calls it:

| Call | Where | When |
|---|---|---|
| `getTableSchema` / `tableSchemaLoaded` | `LakeVillaMetadata` | first use of a table in a transaction (lookup; after a miss, the schema LakeVilla loaded) |
| `getTableRows` / `tableRowsRead` | `LakeVillaTransaction.readTable` | every scan, after the snapshot is taken (lookup; after a miss, the rows LakeVilla read) |
| `tableWritten` | `LakeVillaTransaction` | first write of a transaction to a table |
| `transactionCommitted` / `transactionAborted` | `LakeVillaTransaction` | end of a transaction |
| `tableCreated` / `tableDropped` | `LakeVillaMetadata`, `LakeVillaTransaction` | DDL, and the table of a failed `CREATE TABLE AS` |

The default, `NoOpLakeVillaCache`, misses every lookup and ignores the
notifications. Another implementation is bound in `LakeVillaModule`. The
Javadoc of `LakeVillaCache` lists what an implementation has to consider:
changes by other clients and engines are not notified, a transaction reads its
snapshot plus its own writes, and a row-cache hit skips LakeVilla's read, so
serializable validation no longer sees that the table was read.

