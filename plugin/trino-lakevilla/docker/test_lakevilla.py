#!/usr/bin/env python3
"""End-to-end SQL tests of the LakeVilla connector.

Runs SQL against the Trino server of docker-compose.yml (REST protocol, Python
standard library only) and checks the results: DDL, all column types, CREATE
TABLE AS, INSERT, DELETE (key-based, row-level and all rows), UPDATE, MERGE,
TRUNCATE, multi-statement
and multi-table transactions, snapshot reads, write-write conflicts and write
skew (serializable isolation) and, if the catalog `delta` exists, Delta Lake
interoperability with Trino's own Delta Lake connector.

usage:
  python3 test_lakevilla.py                       # against the running server
  python3 test_lakevilla.py --levels 01,2,012     # restarts Trino per level combination
  python3 test_lakevilla.py -k delete -k conflict # only tests whose name contains these texts

Every test runs in its own schema. The exit code is 1 if a test failed.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))


class QueryError(Exception):
    def __init__(self, name, message):
        super().__init__(f"{name}: {message}")
        self.name = name
        self.message = message


class Session:
    """A Trino client session (keeps the transaction of START TRANSACTION)"""

    def __init__(self, server, catalog="lakevilla", schema="default", user="lakevilla-test"):
        self.server = server.rstrip("/")
        self.catalog = catalog
        self.schema = schema
        self.user = user
        self.transaction = None

    def _request(self, url, data=None):
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
            "X-Trino-Transaction-Id": self.transaction or "NONE",
            # timestamps with their precision (otherwise rounded to milliseconds)
            "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME",
        }
        request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
        for attempt in range(60):
            try:
                with urllib.request.urlopen(request, timeout=300) as response:
                    started = response.headers.get("X-Trino-Started-Transaction-Id")
                    if started:
                        self.transaction = started
                    if response.headers.get("X-Trino-Clear-Transaction-Id"):
                        self.transaction = None
                    return json.loads(response.read())
            except urllib.error.HTTPError as e:
                if e.code == 503:
                    time.sleep(1)
                    continue
                raise
        raise RuntimeError(f"server unavailable: {url}")

    def execute(self, sql):
        """rows of the statement (lists); raises QueryError"""
        result = self._request(f"{self.server}/v1/statement", sql.encode())
        rows = []
        while True:
            if "error" in result:
                error = result["error"]
                raise QueryError(error.get("errorName", "?"), error.get("message", ""))
            rows.extend(result.get("data", []))
            next_uri = result.get("nextUri")
            if not next_uri:
                return rows
            result = self._request(next_uri)

    def value(self, sql):
        rows = self.execute(sql)
        assert len(rows) == 1 and len(rows[0]) == 1, f"expected one value, got {rows}"
        return rows[0][0]


def expect_error(session, sql, name, text=None):
    try:
        session.execute(sql)
    except QueryError as e:
        assert e.name == name, f"expected {name}, got {e}"
        assert text is None or text in e.message, f"expected '{text}' in {e.message}"
        return e
    raise AssertionError(f"expected {name} for: {sql}")


def check(actual, expected, what=""):
    assert actual == expected, f"{what}: expected {expected!r}, got {actual!r}"


# ---------------------------------------------------------------------------
# tests (each gets a session whose schema is new and empty)
# ---------------------------------------------------------------------------

def test_metadata_and_ddl(s, ctx):
    schema = s.schema
    check([schema] in s.execute("SHOW SCHEMAS"), True, "schema listed")
    s.execute("CREATE TABLE t1 (id BIGINT NOT NULL COMMENT 'key', name VARCHAR) COMMENT 'first table'")
    s.execute("CREATE TABLE t2 (id INTEGER)")
    check(sorted(s.execute("SHOW TABLES")), [["t1"], ["t2"]], "tables")
    check(s.execute("DESCRIBE t1"), [["id", "bigint", "", "key"], ["name", "varchar", "", ""]], "columns")
    create = s.value("SHOW CREATE TABLE t1")
    assert "id bigint NOT NULL COMMENT 'key'" in create, create
    rows = s.execute(f"SELECT table_name, column_name, data_type FROM information_schema.columns "
                     f"WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position")
    check(rows, [["t1", "id", "bigint"], ["t1", "name", "varchar"], ["t2", "id", "integer"]], "information_schema")
    expect_error(s, "CREATE TABLE t1 (x INTEGER)", "TABLE_ALREADY_EXISTS")
    s.execute("CREATE TABLE IF NOT EXISTS t1 (x INTEGER)")
    s.execute("DROP TABLE t2")
    check(s.execute("SHOW TABLES"), [["t1"]], "tables after drop")
    expect_error(s, "DROP TABLE t2", "TABLE_NOT_FOUND")
    expect_error(s, "CREATE TABLE t3 (x TIME)", "NOT_SUPPORTED")
    expect_error(s, f"DROP SCHEMA {schema}", "SCHEMA_NOT_EMPTY")
    expect_error(s, "SELECT * FROM t1 FOR VERSION AS OF 1", "NOT_SUPPORTED")


def test_types(s, ctx):
    s.execute("""CREATE TABLE types (
        k BIGINT, b BOOLEAN, ti TINYINT, si SMALLINT, i INTEGER, r REAL, d DOUBLE,
        dec_short DECIMAL(10,3), dec_long DECIMAL(38,6), v VARCHAR, c CHAR(3), vb VARBINARY,
        dt DATE, ts TIMESTAMP(6), tstz TIMESTAMP(6) WITH TIME ZONE, ts3 TIMESTAMP(3))""")
    check(s.value("SELECT data_type FROM information_schema.columns WHERE table_name = 'types' AND column_name = 'c'"),
          "varchar", "char stored as varchar")
    check(s.value("SELECT data_type FROM information_schema.columns WHERE table_name = 'types' AND column_name = 'ts3'"),
          "timestamp(6)", "timestamp(3) stored as timestamp(6)")
    s.execute("""INSERT INTO types VALUES
        (1, true, TINYINT '-128', SMALLINT '32767', 2147483647, REAL '1.5', 1e300,
         DECIMAL '-1234567.891', DECIMAL '12345678901234567890123456789012.123456', 'héllo wörld ✓', 'ab', X'00FF10',
         DATE '1500-02-28', TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '2024-05-06 07:08:09.123456 Europe/Berlin',
         TIMESTAMP '2001-02-03 04:05:06.789'),
        (2, false, 0, 0, 0, REAL '-0.25', -2.5, 0, 0, '', 'xyz', X'', DATE '9999-12-31',
         TIMESTAMP '2262-04-11 23:47:16.854775', TIMESTAMP '1900-01-01 00:00:00 UTC', NULL),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""")
    rows = s.execute("SELECT * FROM types ORDER BY k")
    check(rows[0], [1, True, -128, 32767, 2147483647, 1.5, 1e300, "-1234567.891",
                    "12345678901234567890123456789012.123456", "héllo wörld ✓", "ab",
                    base64.b64encode(bytes([0, 255, 16])).decode(), "1500-02-28", "1969-12-31 23:59:59.999999",
                    "2024-05-06 05:08:09.123456 UTC", "2001-02-03 04:05:06.789000"], "row 1")
    check(rows[1], [2, False, 0, 0, 0, -0.25, -2.5, "0.000", "0.000000", "", "xyz", "", "9999-12-31",
                    "2262-04-11 23:47:16.854775", "1900-01-01 00:00:00.000000 UTC", None], "row 2")
    check(rows[2], [3] + [None] * 15, "row 3")
    # predicates are evaluated by Trino
    check(s.value("SELECT count(*) FROM types WHERE dec_short < 0 AND v LIKE 'h%'"), 1, "filter")


def test_not_null(s, ctx):
    s.execute("CREATE TABLE nn (id BIGINT NOT NULL, v VARCHAR)")
    expect_error(s, "INSERT INTO nn VALUES (NULL, 'x')", "CONSTRAINT_VIOLATION")
    s.execute("INSERT INTO nn (id) VALUES (1)")
    check(s.execute("SELECT * FROM nn"), [[1, None]], "missing column is null")


def test_create_table_as(s, ctx):
    s.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation")
    s.execute("CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders")
    check(s.value("SELECT count(*) FROM orders"), 15000, "orders")
    # sums of doubles depend on the order of the rows: compare them rounded
    for query in ["SELECT count(*), round(sum(totalprice), 2), min(orderdate), max(orderdate), count(DISTINCT custkey) FROM {}",
                  "SELECT orderpriority, count(*), round(sum(totalprice), 2), checksum(comment) FROM {} GROUP BY 1 ORDER BY 1"]:
        check(s.execute(query.format("orders")), s.execute(query.format("tpch.tiny.orders")), query)
    check(s.execute("SELECT * FROM nation ORDER BY nationkey"),
          s.execute("SELECT * FROM tpch.tiny.nation ORDER BY nationkey"), "nation")
    # a join of two LakeVilla tables with a TPC-H table
    query = ("SELECT n.name, count(*) FROM {o} o JOIN tpch.tiny.customer c ON o.custkey = c.custkey "
             "JOIN {n} n ON c.nationkey = n.nationkey GROUP BY 1 ORDER BY 1")
    check(s.execute(query.format(o="orders", n="nation")),
          s.execute(query.format(o="tpch.tiny.orders", n="tpch.tiny.nation")), "join")


def test_create_table_as_failure_drops_table(s, ctx):
    expect_error(s, "CREATE TABLE broken AS SELECT 10 / (nationkey - 5) AS x FROM tpch.tiny.nation", "DIVISION_BY_ZERO")
    check(s.execute("SHOW TABLES"), [], "no table after the failed CREATE TABLE AS")


def test_insert(s, ctx):
    s.execute("CREATE TABLE items (id BIGINT, name VARCHAR, price DOUBLE)")
    s.execute("INSERT INTO items VALUES (1, 'a', 1.0)")
    s.execute("INSERT INTO items (price, id) VALUES (2.5, 2)")
    s.execute("INSERT INTO items SELECT orderkey + 100, orderstatus, totalprice FROM tpch.tiny.orders")
    check(s.value("SELECT count(*) FROM items"), 15002, "rows")
    check(s.execute("SELECT * FROM items WHERE id <= 2 ORDER BY id"), [[1, "a", 1.0], [2, None, 2.5]], "rows 1, 2")
    # INSERT reading the same table
    s.execute("INSERT INTO items SELECT id + 1000000, name, price FROM items")
    check(s.value("SELECT count(*) FROM items"), 30004, "rows after self insert")


def test_delete(s, ctx):
    s.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation")
    s.execute("INSERT INTO nation SELECT * FROM tpch.tiny.nation WHERE nationkey < 3")
    check(s.value("SELECT count(*) FROM nation"), 28, "rows")
    # key-based removal: the first column (nationkey)
    check(s.execute("DELETE FROM nation WHERE nationkey = 1"), [[2]], "delete key 1 (in two files)")
    check(s.execute("DELETE FROM nation WHERE nationkey IN (5, 6, 1000)"), [[2]], "delete keys")
    check(s.execute("DELETE FROM nation WHERE nationkey = 1000"), [[0]], "delete missing key")
    # consecutive keys are a range in Trino
    check(s.execute("DELETE FROM nation WHERE nationkey BETWEEN 8 AND 9"), [[2]], "delete key range")
    check(s.value("SELECT count(*) FROM nation"), 22, "rows after deletes")
    check(s.value("SELECT count(*) FROM nation WHERE nationkey IN (1, 5, 6, 8, 9)"), 0, "deleted keys")
    # the scan returns the keys of a key predicate
    check(s.execute("SELECT nationkey, name FROM nation WHERE nationkey IN (0, 7) ORDER BY 1"),
          [[0, "ALGERIA"], [0, "ALGERIA"], [7, "GERMANY"]], "key lookup")
    # other conditions: row-level removal (rows by data file and position)
    check(s.execute("DELETE FROM nation WHERE name = 'GERMANY'"), [[1]], "delete by another column")
    check(s.execute("DELETE FROM nation WHERE nationkey = 0 AND regionkey = 0"), [[2]], "delete by key and another column")
    check(s.value("SELECT count(*) FROM nation"), 19, "rows after row-level deletes")
    check(s.execute("DELETE FROM nation"), [[19]], "delete all")
    check(s.value("SELECT count(*) FROM nation"), 0, "empty")
    s.execute("INSERT INTO nation SELECT * FROM tpch.tiny.nation")
    s.execute("TRUNCATE TABLE nation")
    check(s.value("SELECT count(*) FROM nation"), 0, "truncated")
    # string and date keys
    s.execute("CREATE TABLE skeys (k VARCHAR, v INTEGER)")
    s.execute("INSERT INTO skeys VALUES ('a', 1), ('b', 2), ('b', 3), ('c', 4)")
    check(s.execute("DELETE FROM skeys WHERE k = 'b'"), [[2]], "delete string key")
    check(s.execute("SELECT * FROM skeys ORDER BY k"), [["a", 1], ["c", 4]], "string keys")
    s.execute("CREATE TABLE dkeys (k DATE, v INTEGER)")
    s.execute("INSERT INTO dkeys VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 2)")
    check(s.execute("DELETE FROM dkeys WHERE k = DATE '2024-01-02'"), [[1]], "delete date key")
    check(s.execute("SELECT v FROM dkeys"), [[1]], "date keys")


def test_transactions(s, ctx):
    s.execute("CREATE TABLE a (id BIGINT, v VARCHAR)")
    s.execute("CREATE TABLE b (id BIGINT, v VARCHAR)")
    # rollback of a multi-table transaction
    s.execute("START TRANSACTION")
    s.execute("INSERT INTO a VALUES (1, 'a1')")
    s.execute("INSERT INTO b VALUES (1, 'b1')")
    # a transaction reads its own writes
    check(s.value("SELECT count(*) FROM a"), 1, "own write")
    s.execute("ROLLBACK")
    check(s.value("SELECT count(*) FROM a") + s.value("SELECT count(*) FROM b"), 0, "rolled back")
    # commit of a multi-table transaction: both writes become visible
    s.execute("START TRANSACTION")
    s.execute("INSERT INTO a VALUES (1, 'a1')")
    s.execute("INSERT INTO b VALUES (1, 'b1')")
    s.execute("DELETE FROM a WHERE id = 1")
    s.execute("INSERT INTO a VALUES (1, 'a2')")
    other = Session(s.server, schema=s.schema)
    check(other.value("SELECT count(*) FROM b"), 0, "uncommitted write invisible to others")
    s.execute("COMMIT")
    check(s.execute("SELECT * FROM a"), [[1, "a2"]], "committed a")
    check(s.execute("SELECT * FROM b"), [[1, "b1"]], "committed b")
    # isolation levels: SERIALIZABLE is LakeVilla's default (ISOLATION=serializable)
    s.execute("START TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    check(s.value("SELECT count(*) FROM a"), 1, "serializable transaction")
    s.execute("COMMIT")
    # a failed statement fails the transaction
    s.execute("START TRANSACTION")
    s.execute("INSERT INTO a VALUES (2, 'x')")
    expect_error(s, "INSERT INTO a SELECT 1 / 0, 'y'", "DIVISION_BY_ZERO")
    try:
        s.execute("COMMIT")
    except QueryError:
        pass
    s.transaction = None
    check(s.value("SELECT count(*) FROM a"), 1, "failed transaction not committed")


def test_snapshot_reads(s, ctx):
    s.execute("CREATE TABLE t (id BIGINT)")
    s.execute("INSERT INTO t VALUES (1), (2)")
    s.execute("START TRANSACTION")
    check(s.value("SELECT count(*) FROM t"), 2, "first read")
    # a concurrent writer (with markers, its commit waits for this transaction)
    writer = Session(s.server, schema=s.schema)
    result = {}

    def write():
        try:
            writer.execute("INSERT INTO t VALUES (3)")
            result["ok"] = True
        except Exception as e:  # noqa: BLE001
            result["error"] = e

    thread = threading.Thread(target=write)
    thread.start()
    time.sleep(3)
    check(s.value("SELECT count(*) FROM t"), 2, "repeatable read")
    s.execute("COMMIT")
    thread.join(timeout=120)
    assert not thread.is_alive(), "concurrent insert did not finish"
    assert result.get("ok"), f"concurrent insert failed: {result.get('error')}"
    check(s.value("SELECT count(*) FROM t"), 3, "after both committed")


def run_concurrent_commits(s, t1_statements, t2_statements):
    """runs both transactions interleaved; returns (t1 error, t2 error) of the commits"""
    t1 = Session(s.server, schema=s.schema)
    t2 = Session(s.server, schema=s.schema)
    t1.execute("START TRANSACTION")
    t2.execute("START TRANSACTION")
    for a, b in zip(t1_statements, t2_statements):
        if a:
            t1.execute(a)
        if b:
            t2.execute(b)
    errors = []
    for t in (t1, t2):
        try:
            t.execute("COMMIT")
            errors.append(None)
        except QueryError as e:
            errors.append(e)
            t.transaction = None
    return errors


def test_write_conflict(s, ctx):
    # lost update: both transactions replace the row of key 1
    s.execute("CREATE TABLE acc (id BIGINT, balance BIGINT)")
    s.execute("INSERT INTO acc VALUES (1, 100), (2, 100)")
    errors = run_concurrent_commits(
        s,
        ["SELECT balance FROM acc WHERE id = 1", "DELETE FROM acc WHERE id = 1", "INSERT INTO acc VALUES (1, 110)"],
        ["SELECT balance FROM acc WHERE id = 1", "DELETE FROM acc WHERE id = 1", "INSERT INTO acc VALUES (1, 120)"])
    committed = [e is None for e in errors]
    check(committed, [True, False], "the first committer wins")
    check(errors[1].name, "TRANSACTION_CONFLICT", "the second transaction aborts")
    check(s.execute("SELECT * FROM acc ORDER BY id"), [[1, 110], [2, 100]], "no lost update")


def test_write_skew(s, ctx):
    # T1 reads x and y and writes y; T2 reads x and y and writes x: serializable
    # isolation aborts one of them (snapshot isolation would commit both)
    s.execute("CREATE TABLE x (v BIGINT)")
    s.execute("CREATE TABLE y (v BIGINT)")
    s.execute("INSERT INTO x VALUES (1)")
    s.execute("INSERT INTO y VALUES (1)")
    errors = run_concurrent_commits(
        s,
        ["SELECT sum(v) FROM x", "SELECT sum(v) FROM y", "INSERT INTO y VALUES (-1)"],
        ["SELECT sum(v) FROM x", "SELECT sum(v) FROM y", "INSERT INTO x VALUES (-1)"])
    check(sum(e is None for e in errors), 1, f"exactly one transaction commits ({errors})")
    check(s.value("SELECT (SELECT sum(v) FROM x) + (SELECT sum(v) FROM y)"), 1, "invariant x + y >= 1")


def test_update(s, ctx):
    s.execute("CREATE TABLE nation AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation")
    s.execute("INSERT INTO nation SELECT nationkey + 100, name, regionkey, comment FROM tpch.tiny.nation WHERE nationkey < 5")
    check(s.value("SELECT count(*) FROM nation"), 30, "rows")
    # by key (pushed into the scan), by another column, with expressions of the row
    check(s.execute("UPDATE nation SET name = 'Germany' WHERE nationkey = 7"), [[1]], "update by key")
    check(s.execute("UPDATE nation SET regionkey = regionkey + 10, comment = upper(name) WHERE regionkey = 1"), [[8]],
          "update by another column (rows in two files)")
    check(s.execute("UPDATE nation SET name = 'x' WHERE nationkey = 1000"), [[0]], "update of no row")
    check(s.value("SELECT name FROM nation WHERE nationkey = 7"), "Germany", "updated by key")
    check(s.execute("SELECT nationkey, comment FROM nation WHERE regionkey = 11 ORDER BY 1"),
          [[1, "ARGENTINA"], [2, "BRAZIL"], [3, "CANADA"], [17, "PERU"], [24, "UNITED STATES"],
           [101, "ARGENTINA"], [102, "BRAZIL"], [103, "CANADA"]], "updated rows")
    # the other rows of the rewritten files are unchanged
    query = "SELECT count(*), checksum(name), checksum(comment), sum(regionkey) FROM {} WHERE {}"
    check(s.execute(query.format("nation", "nationkey < 100 AND regionkey <> 11 AND nationkey <> 7")),
          s.execute(query.format("tpch.tiny.nation", "regionkey <> 1 AND nationkey <> 7")), "unchanged rows")
    check(s.value("SELECT count(*) FROM nation"), 30, "no row lost or duplicated")
    # all rows, and the key column itself
    check(s.execute("UPDATE nation SET comment = 'all'"), [[30]], "update all rows")
    check(s.execute("UPDATE nation SET nationkey = nationkey + 1000 WHERE nationkey >= 100"), [[5]], "update the key")
    check(s.execute("SELECT count(*), count(DISTINCT nationkey), min(comment), max(comment), max(nationkey) FROM nation"),
          [[30, 30, "all", "all", 1104]], "after updating all rows")
    # the key-based DELETE still works on rewritten files
    check(s.execute("DELETE FROM nation WHERE nationkey IN (1101, 1102)"), [[2]], "key delete after updates")


def test_update_keeps_all_types(s, ctx):
    s.execute("""CREATE TABLE types (
        k BIGINT, b BOOLEAN, ti TINYINT, si SMALLINT, i INTEGER, r REAL, d DOUBLE,
        dec_short DECIMAL(10,3), dec_long DECIMAL(38,6), v VARCHAR, vb VARBINARY,
        dt DATE, ts TIMESTAMP(6), tstz TIMESTAMP(6) WITH TIME ZONE)""")
    s.execute("""INSERT INTO types VALUES
        (1, true, TINYINT '-128', SMALLINT '32767', 2147483647, REAL '1.5', 1e300,
         DECIMAL '-1234567.891', DECIMAL '12345678901234567890123456789012.123456', 'héllo ✓', X'00FF10',
         DATE '1500-02-28', TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '2024-05-06 07:08:09.123456 UTC'),
        (2, false, 0, 0, 0, REAL '-0.25', -2.5, 0, 0, 'second', X'', DATE '9999-12-31',
         TIMESTAMP '2262-04-11 23:47:16.854775', TIMESTAMP '1900-01-01 00:00:00 UTC'),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""")
    before = s.execute("SELECT * FROM types ORDER BY k")
    # the file is rewritten: the other rows and columns keep their values
    check(s.execute("UPDATE types SET v = 'changed' WHERE k = 2"), [[1]], "update")
    after = s.execute("SELECT * FROM types ORDER BY k")
    check(after[0], before[0], "row 1")
    check(after[2], before[2], "row 3")
    check(after[1][:9] + after[1][10:], before[1][:9] + before[1][10:], "row 2 except v")
    check(after[1][9], "changed", "row 2 v")


def test_merge(s, ctx):
    s.execute("CREATE TABLE target (id BIGINT, v VARCHAR, n INTEGER)")
    s.execute("INSERT INTO target VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
    s.execute("CREATE TABLE source (id BIGINT, v VARCHAR)")
    s.execute("INSERT INTO source VALUES (1, 'A'), (3, 'DEL'), (4, 'D')")
    check(s.execute("""MERGE INTO target t USING source s ON t.id = s.id
        WHEN MATCHED AND s.v = 'DEL' THEN DELETE
        WHEN MATCHED THEN UPDATE SET v = s.v, n = t.n + 10
        WHEN NOT MATCHED THEN INSERT (id, v, n) VALUES (s.id, s.v, 0)"""), [[3]], "merge")
    check(s.execute("SELECT * FROM target ORDER BY id"), [[1, "A", 11], [2, "b", 2], [4, "D", 0]], "merged")
    # DELETE with a subquery
    check(s.execute("DELETE FROM target WHERE id IN (SELECT id FROM source)"), [[2]], "delete with a subquery")
    check(s.execute("SELECT * FROM target"), [[2, "b", 2]], "after delete")


def test_row_changes_in_transactions(s, ctx):
    s.execute("CREATE TABLE t (id BIGINT, v VARCHAR)")
    s.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    s.execute("START TRANSACTION")
    s.execute("INSERT INTO t VALUES (3, 'c')")
    # rows of a committed file and of the transaction's own file
    check(s.execute("UPDATE t SET v = 'x' WHERE v IN ('b', 'c')"), [[2]], "update in a transaction")
    check(s.execute("SELECT * FROM t ORDER BY id"), [[1, "a"], [2, "x"], [3, "x"]], "own changes")
    check(Session(s.server, schema=s.schema).execute("SELECT * FROM t ORDER BY id"), [[1, "a"], [2, "b"]],
          "invisible to others")
    s.execute("ROLLBACK")
    check(s.execute("SELECT * FROM t ORDER BY id"), [[1, "a"], [2, "b"]], "rolled back")
    s.execute("START TRANSACTION")
    s.execute("UPDATE t SET v = 'y' WHERE id = 1")
    s.execute("DELETE FROM t WHERE v = 'b'")
    s.execute("UPDATE t SET v = v || '!'")
    s.execute("COMMIT")
    check(s.execute("SELECT * FROM t"), [[1, "y!"]], "committed")


def test_concurrent_updates(s, ctx):
    s.execute("CREATE TABLE acc (id BIGINT, balance BIGINT)")
    s.execute("INSERT INTO acc VALUES (1, 100), (2, 100)")
    # lost update: both transactions increase the balance of account 1
    errors = run_concurrent_commits(s, ["UPDATE acc SET balance = balance + 10 WHERE id = 1"],
                                    ["UPDATE acc SET balance = balance + 20 WHERE id = 1"])
    check([e is None for e in errors], [True, False], "the first committer wins")
    check(errors[1].name, "TRANSACTION_CONFLICT", "the second transaction aborts")
    check(s.execute("SELECT * FROM acc ORDER BY id"), [[1, 110], [2, 100]], "no lost update")
    # different rows of the same data file: conflicts are detected per file (as in Delta)
    errors = run_concurrent_commits(s, ["UPDATE acc SET balance = 0 WHERE id = 1"],
                                    ["UPDATE acc SET balance = 0 WHERE id = 2"])
    check(sum(e is None for e in errors), 1, f"one of the transactions commits ({errors})")
    check(s.value("SELECT count(*) FROM acc"), 2, "no row lost or duplicated")
    # different tables: both commit
    s.execute("CREATE TABLE acc2 AS SELECT * FROM acc")
    errors = run_concurrent_commits(s, ["UPDATE acc SET balance = 1"], ["UPDATE acc2 SET balance = 2"])
    check(errors, [None, None], "updates of different tables")
    check(s.value("SELECT sum(balance) FROM acc") + s.value("SELECT sum(balance) FROM acc2"), 6, "both updates")


def test_drop_schema_cascade(s, ctx):
    s.execute("CREATE TABLE t (id BIGINT)")
    s.execute("INSERT INTO t VALUES (1)")
    s.execute(f"DROP SCHEMA {s.schema} CASCADE")
    check([s.schema] in s.execute("SHOW SCHEMAS"), False, "schema dropped")
    expect_error(s, "SELECT * FROM t", "SCHEMA_NOT_FOUND")
    ctx["dropped"] = True


def test_delta_interoperability(s, ctx):
    """tables of LakeVilla read by Trino's Delta Lake connector and vice versa"""
    if not ctx["delta"]:
        raise Skip("no catalog delta")
    schema = s.schema
    delta = Session(s.server, catalog="delta", schema=schema)
    location = f"s3://{ctx['bucket']}/wh/{schema}.db"
    delta.execute(f"CREATE SCHEMA delta.{schema} WITH (location = '{location}')")
    try:
        # written by LakeVilla, read by the Delta Lake connector
        s.execute("CREATE TABLE lv_orders AS SELECT orderkey, custkey, totalprice, orderdate, comment FROM tpch.tiny.orders")
        s.execute("DELETE FROM lv_orders WHERE orderkey IN (1, 2, 3)")
        s.execute("CREATE TABLE lv_types (k BIGINT, d DECIMAL(38,6), ts TIMESTAMP(6) WITH TIME ZONE, ntz TIMESTAMP(6), dt DATE, b VARBINARY)")
        s.execute("INSERT INTO lv_types VALUES (1, DECIMAL '-12345678901234567890.123456', "
                  "TIMESTAMP '2024-05-06 07:08:09.123456 UTC', TIMESTAMP '1969-12-31 23:59:59.999999', DATE '1500-02-28', X'00FF')")
        delta.execute(f"CALL delta.system.register_table('{schema}', 'lv_orders', '{location}/lv_orders')")
        delta.execute(f"CALL delta.system.register_table('{schema}', 'lv_types', '{location}/lv_types')")
        query = "SELECT count(*), round(sum(totalprice), 2), max(comment), checksum(comment) FROM {}"
        check(delta.execute(query.format("lv_orders")), s.execute(query.format("lv_orders")), "LakeVilla table read by Delta")
        check(delta.value("SELECT count(*) FROM lv_orders"), 14997, "rows")
        # the Delta connector reads Delta's timestamp as timestamp(3) with time zone (milliseconds)
        lv_types = s.execute("SELECT k, CAST(d AS VARCHAR), CAST(ntz AS VARCHAR), CAST(dt AS VARCHAR), b, "
                             "to_unixtime(CAST(ts AS TIMESTAMP(3) WITH TIME ZONE)) FROM lv_types")
        delta_types = delta.execute("SELECT k, CAST(d AS VARCHAR), CAST(ntz AS VARCHAR), CAST(dt AS VARCHAR), b, to_unixtime(ts) FROM lv_types")
        check(delta_types, lv_types, "types read by Delta")
        # written by the Delta Lake connector, read and changed by LakeVilla
        # (an external table: the file metastore keeps managed tables in its own directory)
        delta.execute(f"CREATE TABLE delta_nation WITH (location = '{location}/delta_nation') AS SELECT * FROM tpch.tiny.nation")
        delta.execute("INSERT INTO delta_nation SELECT * FROM tpch.tiny.nation WHERE nationkey < 5")
        check(s.execute("SELECT * FROM delta_nation ORDER BY nationkey, name"),
              delta.execute("SELECT * FROM delta_nation ORDER BY nationkey, name"), "Delta table read by LakeVilla")
        check(s.execute("DELETE FROM delta_nation WHERE nationkey = 1"), [[2]], "LakeVilla deletes in a Delta table")
        s.execute("INSERT INTO delta_nation VALUES (100, 'LAKEVILLA', 0, 'inserted by LakeVilla')")
        check(delta.value("SELECT count(*) FROM delta_nation"), 29, "Delta reads LakeVilla's changes")
        check(delta.value("SELECT name FROM delta_nation WHERE nationkey = 100"), "LAKEVILLA", "LakeVilla row")
        # a commit of the Delta connector after LakeVilla's commits
        delta.execute("INSERT INTO delta_nation VALUES (101, 'DELTA', 0, 'inserted by Delta')")
        check(s.value("SELECT count(*) FROM delta_nation"), 30, "LakeVilla reads the Delta commit")
        # a LakeVilla commit after the Delta commit (level 2: the global log is repaired)
        s.execute("INSERT INTO delta_nation VALUES (102, 'LAKEVILLA2', 0, 'inserted by LakeVilla')")
        check(delta.value("SELECT count(*) FROM delta_nation"), 31, "Delta reads LakeVilla's later commit")
        # UPDATE by LakeVilla (files rewritten by LakeVilla), read by Delta
        check(s.execute("UPDATE delta_nation SET comment = 'updated by LakeVilla' WHERE regionkey = 2"), [[5]],
              "LakeVilla updates a Delta table")
        check(delta.value("SELECT count(*) FROM delta_nation WHERE comment = 'updated by LakeVilla'"), 5,
              "Delta reads LakeVilla's update")
        check(delta.value("SELECT count(*) FROM delta_nation"), 31, "rows after LakeVilla's update")
        # UPDATE by Delta of a LakeVilla table, read by LakeVilla
        rows = s.value("SELECT count(*) FROM lv_orders WHERE orderkey < 100")
        delta.execute("UPDATE lv_orders SET comment = 'updated by Delta' WHERE orderkey < 100")
        check(s.value("SELECT count(*) FROM lv_orders WHERE comment = 'updated by Delta'"), rows,
              "LakeVilla reads Delta's update")
        check(s.value("SELECT count(*) FROM lv_orders"), 14997, "rows after Delta's update")
    finally:
        for table in ["lv_orders", "lv_types", "delta_nation"]:
            # the tables stay in the warehouse (dropped with the LakeVilla schema)
            try:
                delta.execute(f"CALL delta.system.unregister_table('{schema}', '{table}')")
            except QueryError:
                pass
        try:
            delta.execute(f"DROP SCHEMA delta.{schema}")
        except QueryError:
            pass


class Skip(Exception):
    pass


TESTS = [
    test_metadata_and_ddl,
    test_types,
    test_not_null,
    test_create_table_as,
    test_create_table_as_failure_drops_table,
    test_insert,
    test_delete,
    test_transactions,
    test_snapshot_reads,
    test_write_conflict,
    test_write_skew,
    test_update,
    test_update_keeps_all_types,
    test_merge,
    test_row_changes_in_transactions,
    test_concurrent_updates,
    test_drop_schema_cascade,
    test_delta_interoperability,
]


def wait_for_server(server, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{server}/v1/info", timeout=5) as response:
                if not json.loads(response.read()).get("starting", True):
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(2)
    raise RuntimeError(f"Trino at {server} did not start")


def restart_trino(levels):
    env = dict(os.environ, LAKEVILLA_LEVELS=levels)
    subprocess.run(["docker", "compose", "-f", os.path.join(HERE, "docker-compose.yml"), "up", "-d",
                    "--force-recreate", "--no-build", "trino"], check=True, env=env,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_tests(server, filters, label):
    probe = Session(server)
    catalogs = [row[0] for row in probe.execute("SHOW CATALOGS")]
    ctx_base = {"delta": "delta" in catalogs, "bucket": "warehouse"}
    results = []
    for test in TESTS:
        name = test.__name__
        if filters and not any(f in name for f in filters):
            continue
        schema = f"e2e_{uuid.uuid4().hex[:8]}"
        session = Session(server, schema=schema)
        ctx = dict(ctx_base)
        start = time.time()
        try:
            session.execute(f"CREATE SCHEMA {schema}")
            test(session, ctx)
            status, detail = "PASS", ""
        except Skip as e:
            status, detail = "SKIP", str(e)
        except Exception as e:  # noqa: BLE001
            status, detail = "FAIL", f"{e}\n{traceback.format_exc()}"
        finally:
            if not ctx.get("dropped"):
                try:
                    Session(server, schema=schema).execute(f"DROP SCHEMA {schema} CASCADE")
                except QueryError:
                    pass
        print(f"[{label}] {status} {name} ({time.time() - start:.1f}s){' ' + detail if detail else ''}", flush=True)
        results.append((label, name, status))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", default=f"http://localhost:{os.environ.get('TRINO_PORT', '8080')}")
    parser.add_argument("--levels", help="comma-separated level combinations; restarts Trino for each (docker compose)")
    parser.add_argument("-k", action="append", default=[], help="only tests whose name contains the text")
    args = parser.parse_args()

    results = []
    if args.levels:
        for levels in args.levels.split(","):
            restart_trino(levels)
            wait_for_server(args.server)
            results += run_tests(args.server, args.k, f"levels {levels}")
    else:
        wait_for_server(args.server)
        results += run_tests(args.server, args.k, "running server")

    failed = [r for r in results if r[2] == "FAIL"]
    passed = sum(r[2] == "PASS" for r in results)
    skipped = sum(r[2] == "SKIP" for r in results)
    print(f"\n{passed} passed, {len(failed)} failed, {skipped} skipped")
    for label, name, _ in failed:
        print(f"  FAILED [{label}] {name}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
