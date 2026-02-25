"""Tests for anti-fraud Memgraph schema, triggers, and data integrity.

Requires:
    - Running Memgraph instance (bolt://localhost:7687 by default)
    - pip install neo4j pytest

Configure via env vars:
    MEMGRAPH_HOST  (default: localhost)
    MEMGRAPH_PORT  (default: 7687)

Run:
    pytest tests/test_schema.py -v
"""

import os
import time
from pathlib import Path

import pytest

try:
    from neo4j import GraphDatabase
except ImportError:
    pytest.skip(
        "neo4j driver required: pip install neo4j",
        allow_module_level=True,
    )

# ── paths & constants ─────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE   = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "schema" / "triggers.cypher"

INDEXED_PROPS = [
    "id", "phone", "email", "document", "ip",
    "wallet", "payment_wallet", "address",
    "status", "cluster_id", "fraud_score", "created_at",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_cypher(path: Path) -> list[str]:
    """Parse a .cypher file into executable statements.

    Strips ``//`` comment lines and splits on ``;``.
    NOTE: does not handle ``//`` inside quoted strings —
    our .cypher files never contain URLs in literals, so this is safe.
    """
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if stripped.startswith("//"):
            continue
        # strip trailing inline comment (safe for our files)
        idx = raw.find("//")
        if idx != -1:
            raw = raw[:idx]
        lines.append(raw)
    text = "\n".join(lines)
    return [s.strip() for s in text.split(";") if s.strip()]


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mg_driver():
    """Session-scoped Bolt driver; skips the entire suite if Memgraph is down."""
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    driver = GraphDatabase.driver(
        f"bolt://{host}:{port}",
        auth=("", ""),  # Memgraph default: no auth
    )
    try:
        with driver.session() as s:
            s.run("RETURN 1").consume()
    except Exception as exc:
        driver.close()
        pytest.skip(f"Memgraph unavailable at {host}:{port}: {exc}")
    yield driver
    driver.close()


@pytest.fixture(scope="session", autouse=True)
def mg_schema(mg_driver):
    """Apply schema.cypher + triggers.cypher once per test session."""
    with mg_driver.session() as s:
        # clean slate
        s.run("MATCH (n) DETACH DELETE n").consume()

        # drop trigger from a possible previous run
        try:
            s.run("DROP TRIGGER link_new_applications").consume()
        except Exception:
            pass

        # indexes & constraints (silently skip "already exists" errors)
        for stmt in _parse_cypher(SCHEMA_FILE):
            try:
                s.run(stmt).consume()
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise

        # trigger
        for stmt in _parse_cypher(TRIGGERS_FILE):
            s.run(stmt).consume()

    yield

    # session teardown
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
        try:
            s.run("DROP TRIGGER link_new_applications").consume()
        except Exception:
            pass


@pytest.fixture(autouse=True)
def mg_clean(mg_driver):
    """Wipe all nodes and edges before every test for isolation."""
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
    yield


# ── 1. indexes ────────────────────────────────────────────────────────────────

def test_indexes_exist(mg_driver):
    """Every property listed in schema.cypher must have a label+property index."""
    with mg_driver.session() as s:
        records = s.run("SHOW INDEX INFO").data()

    indexed = {
        (r["label"], r["property"])
        for r in records
        if r.get("index type") == "label+property"
    }

    for prop in INDEXED_PROPS:
        assert ("Application", prop) in indexed, (
            f"Missing index: :Application({prop})"
        )


# ── 2. constraints ───────────────────────────────────────────────────────────

def test_constraints_exist(mg_driver):
    """Unique constraint on Application.id must be present."""
    with mg_driver.session() as s:
        records = s.run("SHOW CONSTRAINT INFO").data()

    found = False
    for r in records:
        if r.get("constraint type") != "unique":
            continue
        if r.get("label") != "Application":
            continue
        props = r.get("properties", "")
        if isinstance(props, (list, tuple)):
            found = "id" in props
        else:
            found = props == "id"
        if found:
            break

    assert found, "UNIQUE constraint on Application(id) not found"


# ── 3. application properties ────────────────────────────────────────────────

def test_application_properties(mg_driver):
    """Create a fully-populated Application and verify every field round-trips."""
    with mg_driver.session() as s:
        s.run(
            """
            CREATE (:Application {
                id:              $id,
                created_at:      localDateTime('2024-06-15T10:30:00'),
                phone:           '+79001234567',
                email:           'test@example.com',
                document:        '1234 567890',
                ip:              '192.168.1.1',
                wallet:          '0xabc123',
                payment_wallet:  'TABC123',
                address:         'Moscow, Main St 1',
                amount:          50000.0,
                status:          'pending',
                cluster_id:      1,
                fraud_score:     0.85
            })
            """,
            id="test_props_001",
        ).consume()

        rec = s.run(
            "MATCH (a:Application {id: $id}) RETURN a",
            id="test_props_001",
        ).single()

    assert rec is not None, "Application node not found after CREATE"
    a = rec["a"]

    assert a["id"]             == "test_props_001"
    assert a["phone"]          == "+79001234567"
    assert a["email"]          == "test@example.com"
    assert a["document"]       == "1234 567890"
    assert a["ip"]             == "192.168.1.1"
    assert a["wallet"]         == "0xabc123"
    assert a["payment_wallet"] == "TABC123"
    assert a["address"]        == "Moscow, Main St 1"
    assert a["amount"]         == pytest.approx(50000.0)
    assert a["status"]         == "pending"
    assert a["cluster_id"]     == 1
    assert a["fraud_score"]    == pytest.approx(0.85)


# ── 4. LINKED edge creation ──────────────────────────────────────────────────

def test_linked_edge_created(mg_driver):
    """Two apps sharing one phone must be connected by a LINKED edge."""
    with mg_driver.session() as s:
        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79001111111', email: 'a@test.com',
                document: '1111 111111', amount: 10000.0, status: 'pending'
            })
            """,
            id="link_001",
        ).consume()

        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79001111111', email: 'b@test.com',
                document: '2222 222222', amount: 20000.0, status: 'pending'
            })
            """,
            id="link_002",
        ).consume()

        rec = s.run(
            """
            MATCH (:Application {id: 'link_002'})-[r:LINKED]->(:Application {id: 'link_001'})
            RETURN r.weight AS weight, r.shared_attrs AS shared_attrs
            """,
        ).single()

    assert rec is not None, "Trigger did not create LINKED edge"
    assert rec["weight"] == 3, f"phone match → weight=3, got {rec['weight']}"
    assert "phone" in rec["shared_attrs"]


# ── 5. LINKED weight calculation ─────────────────────────────────────────────

def test_linked_weight_correct(mg_driver):
    """Weight must equal the sum of per-attribute weights for shared attributes.

    Shared: phone (3) + email (2) + document (5) = 10.
    """
    with mg_driver.session() as s:
        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79999999999', email: 'shared@test.com',
                document: '9999 999999', amount: 10000.0, status: 'pending'
            })
            """,
            id="w_001",
        ).consume()

        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79999999999', email: 'shared@test.com',
                document: '9999 999999', amount: 20000.0, status: 'pending'
            })
            """,
            id="w_002",
        ).consume()

        rec = s.run(
            """
            MATCH (:Application {id: 'w_002'})-[r:LINKED]->(:Application {id: 'w_001'})
            RETURN r.weight AS weight, r.shared_attrs AS shared_attrs
            """,
        ).single()

    assert rec is not None, "LINKED edge not created"
    # phone(3) + email(2) + document(5) = 10
    assert rec["weight"] == 10, f"phone+email+document → weight=10, got {rec['weight']}"
    assert set(rec["shared_attrs"]) == {"phone", "email", "document"}


# ── 6. no link without shared attributes ──────────────────────────────────────

def test_no_link_without_shared(mg_driver):
    """Fully distinct applications must NOT be linked."""
    with mg_driver.session() as s:
        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79000000001', email: 'x@test.com',
                document: '0001 000001', amount: 10000.0, status: 'pending'
            })
            """,
            id="no_001",
        ).consume()

        s.run(
            """
            CREATE (:Application {
                id: $id, created_at: localDateTime(),
                phone: '+79000000002', email: 'y@test.com',
                document: '0002 000002', amount: 20000.0, status: 'pending'
            })
            """,
            id="no_002",
        ).consume()

        rec = s.run(
            """
            MATCH (:Application {id: 'no_001'})-[r:LINKED]-(:Application {id: 'no_002'})
            RETURN count(r) AS cnt
            """,
        ).single()

    assert rec["cnt"] == 0, "LINKED edge should not exist between distinct apps"


# ── 7. bulk insert performance ────────────────────────────────────────────────

def test_bulk_insert_performance(mg_driver):
    """1000 Applications via UNWIND must complete in under 5 seconds."""
    rows = [
        {
            "id": f"bulk_{i:06d}",
            "phone": f"+7900{i:07d}",
            "email": f"bulk{i}@perf.test",
            "document": f"{1000 + i} {100000 + i}",
            "amount": 10000.0 + i,
            "status": "pending",
        }
        for i in range(1000)
    ]

    start = time.perf_counter()
    with mg_driver.session() as s:
        s.run(
            """
            UNWIND $rows AS row
            CREATE (a:Application)
            SET a.id         = row.id,
                a.created_at = localDateTime(),
                a.phone      = row.phone,
                a.email      = row.email,
                a.document   = row.document,
                a.amount     = row.amount,
                a.status     = row.status
            """,
            rows=rows,
        ).consume()
    elapsed = time.perf_counter() - start

    with mg_driver.session() as s:
        cnt = s.run(
            "MATCH (a:Application) WHERE a.id STARTS WITH 'bulk_' "
            "RETURN count(a) AS n",
        ).single()["n"]

    assert cnt == 1000, f"Expected 1000 nodes, got {cnt}"
    assert elapsed < 5.0, (
        f"Bulk insert took {elapsed:.2f}s, expected < 5s"
    )
