"""Tests for FraudGraphClient (graph_client.py).

Requires a running Memgraph instance:
    MEMGRAPH_HOST  (default: localhost)
    MEMGRAPH_PORT  (default: 7687)

Run all tests:       pytest tests/test_client.py -v
Run slow tests only: pytest tests/test_client.py -v -m slow
Skip slow tests:     pytest tests/test_client.py -v -m "not slow"
"""
from __future__ import annotations

import inspect
import os
import sys
import time
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from neo4j import GraphDatabase
except ImportError:
    pytest.skip("neo4j driver required: pip install neo4j", allow_module_level=True)

try:
    from graph_client import FraudGraphClient
except ImportError:
    pytest.skip("graph_client not found in project root", allow_module_level=True)

SCHEMA_FILE = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "triggers.cypher"

# ── shared constants ──────────────────────────────────────────────────────────

# All test dates must be within the trigger's 2-year lookback window.
# Cutoff ≈ 2024-02-25 (today 2026-02-25 − 730 days); use 2024-06-15 for margin.
_DEFAULT_DATE = "2024-06-15T12:00:00"

_SHARED_PHONE = "+79000LINK000"  # phone shared by the linked group in sample_applications
_CLUSTER_ID = 42                 # cluster_id used by fraud_cluster


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_cypher(path: Path) -> list[str]:
    """Split a .cypher file into executable statements, stripping // comments."""
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if raw.strip().startswith("//"):
            continue
        idx = raw.find("//")
        lines.append(raw[:idx] if idx != -1 else raw)
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def _make_app(app_id: str, **overrides: Any) -> dict[str, Any]:
    """Return an Application dict with unique-by-id defaults.

    Uniqueness relies on the id string so phone/email/document never
    accidentally collide between unrelated test apps.
    """
    # Stable pseudo-hash unaffected by PYTHONHASHSEED
    h = sum(ord(c) * (i + 1) for i, c in enumerate(app_id)) % 9_000_000
    base: dict[str, Any] = {
        "id": app_id,
        "phone": f"+7900{h + 1_000_000:07d}",
        "email": f"{app_id.replace(' ', '_')}@test.local",
        "document": f"1000 {h + 100_000:06d}",
        "amount": 10_000.0,
        "status": "pending",
        "created_at": _DEFAULT_DATE,
    }
    base.update(overrides)
    return base


# ── session-level fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def raw_driver():
    """Bare neo4j driver; skips all tests if Memgraph is unreachable."""
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    drv = GraphDatabase.driver(f"bolt://{host}:{port}")
    try:
        with drv.session() as s:
            s.run("RETURN 1").consume()
    except Exception as exc:
        drv.close()
        pytest.skip(f"Memgraph not reachable at {host}:{port}: {exc}")
    yield drv
    drv.close()


@pytest.fixture(scope="session", autouse=True)
def schema_setup(raw_driver):
    """Apply indexes, constraints, and the auto-linking trigger once per session."""
    with raw_driver.session() as s:
        # Drop trigger from any previous run to ensure a clean re-apply.
        try:
            s.run("DROP TRIGGER link_new_applications").consume()
        except Exception:
            pass

        for stmt in _parse_cypher(SCHEMA_FILE):
            try:
                s.run(stmt).consume()
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise

        for stmt in _parse_cypher(TRIGGERS_FILE):
            s.run(stmt).consume()

    yield


# ── per-test fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def db_clean(raw_driver):
    """Wipe all Application nodes (and LINKED edges) before every test."""
    with raw_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
    yield


@pytest.fixture
def client():
    """FraudGraphClient bound to the local Memgraph instance."""
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    with FraudGraphClient(host, port) as c:
        yield c


@pytest.fixture
def sample_applications(client):
    """100 applications: 10 share _SHARED_PHONE (linked by trigger), 90 unique.

    The 10 linked apps accumulate 9 + 8 + ... + 1 = 45 LINKED edges.
    """
    apps: list[dict] = []

    for i in range(10):
        app = _make_app(
            f"linked_{i:03d}",
            phone=_SHARED_PHONE,
            email=f"linked_{i}@sample.test",
            created_at=f"2024-0{(i % 6) + 1}-{(i % 28) + 1:02d}T10:00:00",
        )
        client.add_application(app)
        apps.append(app)

    for i in range(90):
        app = _make_app(f"unique_{i:03d}", created_at=_DEFAULT_DATE)
        client.add_application(app)
        apps.append(app)

    return apps


@pytest.fixture
def fraud_cluster(client):
    """20 applications, all with cluster_id=_CLUSTER_ID.

    Arranged in 5 groups of 4 sharing the same email; the trigger creates
    C(4,2)=6 intra-group LINKED edges per group = 30 edges total.
    Groups are phone-isolated so no inter-group edges form.
    """
    apps: list[dict] = []
    for i in range(20):
        group = i // 4
        app = _make_app(
            f"cluster_{i:03d}",
            phone=f"+7901{i:07d}",                   # unique per app
            email=f"fraudgroup{group}@cluster.test",  # shared within group
            status="fraud",
            cluster_id=_CLUSTER_ID,
            fraud_score=0.9,
            created_at="2024-06-01T12:00:00",
        )
        client.add_application(app)
        apps.append(app)
    return apps


# ── 1. test_add_application ───────────────────────────────────────────────────

def test_add_application(client):
    """add_application persists the node and returns its id."""
    app = _make_app("add_001", phone="+79001000001", status="pending")

    returned_id = client.add_application(app)

    assert returned_id == "add_001"

    ctx = client.get_application_context("add_001")
    assert ctx is not None
    assert ctx["id"] == "add_001"
    assert ctx["phone"] == "+79001000001"
    assert ctx["status"] == "pending"
    assert ctx["amount"] == pytest.approx(10_000.0)


# ── 2. test_linked_created_on_insert ─────────────────────────────────────────

def test_linked_created_on_insert(client):
    """BEFORE-COMMIT trigger must create a LINKED edge for apps sharing a phone."""
    phone = "+79002000002"
    client.add_application(_make_app("lci_a", phone=phone))
    client.add_application(_make_app("lci_b", phone=phone))

    neighbors = list(client.get_neighbors("lci_b", depth=1))
    neighbor_ids = {n["id"] for n in neighbors}

    assert "lci_a" in neighbor_ids, "Trigger did not create LINKED edge"

    link = next(n for n in neighbors if n["id"] == "lci_a")
    assert link["link_weight"] >= 1
    assert "phone" in link["shared_attrs"]


# ── 3. test_find_by_phone ─────────────────────────────────────────────────────

def test_find_by_phone(client, sample_applications):
    """find_by_attribute('phone', ...) returns exactly the 10 linked apps."""
    results = list(client.find_by_attribute("phone", _SHARED_PHONE, limit=50))

    assert len(results) == 10
    for r in results:
        assert r["phone"] == _SHARED_PHONE


# ── 4. test_get_neighbors_depth1 ──────────────────────────────────────────────

def test_get_neighbors_depth1(client):
    """Depth-1 returns only directly linked neighbors."""
    phone = "+79003000003"
    for i in range(4):
        client.add_application(_make_app(f"d1_{i}", phone=phone))

    # From d1_3: trigger created edges to d1_0, d1_1, d1_2
    neighbors = list(client.get_neighbors("d1_3", depth=1))
    ids = {n["id"] for n in neighbors}

    assert ids == {"d1_0", "d1_1", "d1_2"}
    assert "d1_3" not in ids, "Self should not appear in results"
    # Depth-1 query returns relationship properties, not hops
    for n in neighbors:
        assert "link_weight" in n
        assert "shared_attrs" in n


# ── 5. test_get_neighbors_depth2 ──────────────────────────────────────────────

def test_get_neighbors_depth2(client):
    """Depth-2 traversal must include friend-of-friend nodes."""
    # Graph:  d2_a --[phone]--> d2_b --[email]--> d2_c
    # d2_a and d2_c share nothing → no direct edge.
    client.add_application(_make_app(
        "d2_a", phone="+79004000001", email="xa@d2.test",
    ))
    client.add_application(_make_app(
        "d2_b", phone="+79004000001", email="xb_shared@d2.test",
    ))  # trigger: LINKED(d2_b → d2_a) via phone
    client.add_application(_make_app(
        "d2_c", phone="+79004000999", email="xb_shared@d2.test",
    ))  # trigger: LINKED(d2_c → d2_b) via email; no link to d2_a

    depth1_ids = {n["id"] for n in client.get_neighbors("d2_a", depth=1)}
    assert depth1_ids == {"d2_b"}, (
        f"Depth-1 from d2_a should be only {{d2_b}}, got {depth1_ids}"
    )

    depth2_ids = {n["id"] for n in client.get_neighbors("d2_a", depth=2)}
    assert "d2_b" in depth2_ids, "Direct neighbour missing from depth-2"
    assert "d2_c" in depth2_ids, "Friend-of-friend d2_c missing at depth=2"
    assert "d2_a" not in depth2_ids, "Self must not appear in results"

    # Verify hops are correctly reported at depth=2
    depth2 = {n["id"]: n for n in client.get_neighbors("d2_a", depth=2)}
    assert depth2["d2_b"]["hops"] == 1
    assert depth2["d2_c"]["hops"] == 2


# ── 6. test_get_neighbors_limit ───────────────────────────────────────────────

def test_get_neighbors_limit(client):
    """limit parameter must cap the number of returned results."""
    phone = "+79005000005"
    for i in range(10):
        client.add_application(_make_app(f"lim_{i}", phone=phone))

    # lim_9 has 9 neighbours but limit=3 must cut it off
    limited = list(client.get_neighbors("lim_9", depth=1, limit=3))
    assert len(limited) == 3


# ── 7. test_get_application_context ──────────────────────────────────────────

def test_get_application_context(client):
    """Context query returns all app fields plus aggregated neighbour metrics."""
    phone = "+79006000006"
    client.add_application(_make_app("ctx_a", phone=phone, status="pending"))
    client.add_application(_make_app("ctx_b", phone=phone, status="pending"))
    # trigger: LINKED(ctx_b → ctx_a)

    ctx = client.get_application_context("ctx_b")

    assert ctx is not None
    assert ctx["id"] == "ctx_b"
    assert ctx["phone"] == phone
    assert ctx["status"] == "pending"

    # All metric columns must be present
    for key in ("neighbor_count", "fraud_neighbor_count", "max_link_weight", "avg_link_weight"):
        assert key in ctx, f"Missing metric field: {key!r}"

    assert ctx["neighbor_count"] == 1
    assert ctx["fraud_neighbor_count"] == 0      # ctx_a.status == 'pending'
    assert ctx["max_link_weight"] == pytest.approx(1.0)
    assert ctx["avg_link_weight"] == pytest.approx(1.0)

    # Non-existent id returns None
    assert client.get_application_context("does_not_exist") is None


# ── 8. test_generator_memory ─────────────────────────────────────────────────

def test_generator_memory(client):
    """Generator methods must not pre-load all results into memory at creation time."""
    # Bulk-insert 1000 apps with unique attributes (no trigger-created edges).
    rows = [
        {
            "id": f"gm_{i:04d}",
            "phone": f"+7907{i:07d}",
            "email": f"gm{i}@mem.test",
            "cluster_id": 77,
        }
        for i in range(1_000)
    ]
    client.execute_raw(
        "UNWIND $rows AS r "
        "CREATE (:Application {"
        "  id: r.id, created_at: localDateTime(),"
        "  phone: r.phone, email: r.email,"
        "  cluster_id: r.cluster_id,"
        "  amount: 1000.0, status: 'pending'"
        "})",
        {"rows": rows},
    )

    gen = client.get_cluster_members(77, limit=1_000)

    # 1. Must be a true Python generator, not a pre-materialised list.
    assert inspect.isgenerator(gen), (
        f"Expected a generator object, got {type(gen).__name__!r}"
    )

    # 2. The generator frame itself is tiny (< 1 KB) regardless of result count.
    #    A pre-loaded list of 1000 dicts would be orders of magnitude larger.
    assert sys.getsizeof(gen) < 1_024, (
        f"Generator object size {sys.getsizeof(gen)} B suggests results were pre-loaded"
    )

    # 3. Partial iteration works; no errors after early close.
    first = next(gen)
    assert "id" in first
    assert first["id"].startswith("gm_")

    gen.close()  # explicitly release the underlying DB session


# ── 9. test_find_performance ─────────────────────────────────────────────────

@pytest.mark.slow
def test_find_performance(client):
    """1 000 indexed find_by_attribute lookups must complete in < 1 second total."""
    N = 1_000

    # Bulk-insert via UNWIND (all unique attributes → trigger finds no matches).
    rows = [
        {"id": f"fp_{i:04d}", "phone": f"+7906{i:07d}", "email": f"fp{i}@find.test"}
        for i in range(N)
    ]
    client.execute_raw(
        "UNWIND $rows AS r "
        "CREATE (:Application {"
        "  id: r.id, created_at: localDateTime(),"
        "  phone: r.phone, email: r.email,"
        "  amount: 1000.0, status: 'pending'"
        "})",
        {"rows": rows},
    )

    start = time.perf_counter()
    for i in range(N):
        results = list(client.find_by_attribute("phone", f"+7906{i:07d}", limit=5))
        assert len(results) == 1, f"Phone index lookup {i} returned {len(results)} rows"
    elapsed = time.perf_counter() - start

    assert elapsed < 1.0, (
        f"1 000 find_by_attribute calls took {elapsed:.2f} s — expected < 1.0 s"
    )


# ── 10. test_neighbors_performance ───────────────────────────────────────────

@pytest.mark.slow
def test_neighbors_performance(client):
    """get_neighbors(depth=2) on a 10 001-node graph must finish in < 100 ms.

    Graph shape
    -----------
    hub  ──[LINKED]──►  fh_000 … fh_099        (100 direct neighbours)
    fh_i ──[LINKED]──►  sh_{i*99} … sh_{i*99+98}  (99 second-hop each)

    Total nodes : 1 + 100 + 9 900 = 10 001
    Total edges : 100 + 9 900     = 10 000
    """
    HUB = "perf_hub"
    N_FIRST = 100
    N_SECOND = 99   # per first-hop → 9 900 second-hop nodes

    # ── 1. Temporarily disable trigger for fast bulk setup ──────────────────
    #   The BEFORE-COMMIT trigger would do O(N²) attribute checks for a batch
    #   of 10K nodes; disabling it here keeps setup time reasonable.
    client.execute_raw("DROP TRIGGER link_new_applications")
    try:
        # ── 2. Bulk-create all nodes ──────────────────────────────────────────
        all_rows = [{"id": HUB, "phone": "+79990000000", "email": "hub@perf.test"}]
        for i in range(N_FIRST):
            all_rows.append({
                "id": f"fh_{i:03d}",
                "phone": f"+7991{i:07d}",
                "email": f"fh{i}@perf.test",
            })
        for i in range(N_FIRST * N_SECOND):
            all_rows.append({
                "id": f"sh_{i:05d}",
                "phone": f"+7992{i:07d}",
                "email": f"sh{i}@perf.test",
            })

        client.execute_raw(
            "UNWIND $rows AS r "
            "CREATE (:Application {"
            "  id: r.id, created_at: localDateTime(),"
            "  phone: r.phone, email: r.email,"
            "  amount: 1.0, status: 'pending'"
            "})",
            {"rows": all_rows},
        )

        # ── 3. LINKED: hub → 100 first-hop nodes ─────────────────────────────
        client.execute_raw(
            "UNWIND $fh_ids AS fhid "
            "MATCH (h:Application {id: $hub}), (f:Application {id: fhid}) "
            "CREATE (h)-[:LINKED {weight: 1, shared_attrs: ['phone']}]->(f)",
            {"hub": HUB, "fh_ids": [f"fh_{i:03d}" for i in range(N_FIRST)]},
        )

        # ── 4. LINKED: each first-hop → 99 second-hop nodes ──────────────────
        #   Use batches of 2 000 pairs to stay within the 30-second timeout.
        pairs = [
            {"fh": f"fh_{fi:03d}", "sh": f"sh_{fi * N_SECOND + k:05d}"}
            for fi in range(N_FIRST)
            for k in range(N_SECOND)
        ]
        batch_size = 2_000
        for offset in range(0, len(pairs), batch_size):
            client.execute_raw(
                "UNWIND $pairs AS p "
                "MATCH (f:Application {id: p.fh}), (s:Application {id: p.sh}) "
                "CREATE (f)-[:LINKED {weight: 1, shared_attrs: ['email']}]->(s)",
                {"pairs": pairs[offset: offset + batch_size]},
            )

    finally:
        # ── 5. Re-enable trigger so subsequent tests are not affected ─────────
        for stmt in _parse_cypher(TRIGGERS_FILE):
            client.execute_raw(stmt)

    # ── 6. Measure traversal time ─────────────────────────────────────────────
    start = time.perf_counter()
    neighbors = list(client.get_neighbors(HUB, depth=2, limit=10_100))
    elapsed_ms = (time.perf_counter() - start) * 1_000

    found_ids = {n["id"] for n in neighbors}
    assert f"fh_{0:03d}" in found_ids, "Direct neighbour fh_000 not found"
    assert f"sh_{0:05d}" in found_ids, "Second-hop neighbour sh_00000 not found"
    assert len(found_ids) >= N_FIRST, (
        f"Expected at least {N_FIRST} neighbours, got {len(found_ids)}"
    )
    assert elapsed_ms < 100, (
        f"get_neighbors(depth=2) took {elapsed_ms:.1f} ms — expected < 100 ms"
    )
