"""Integration tests for Louvain clustering and cluster metric methods.

Requires:
    - Running Memgraph instance (bolt://localhost:7687 by default)
    - pip install neo4j pytest
    - MAGE community_detection module (tests that run Louvain are auto-skipped
      when MAGE is unavailable)

Configure via env vars:
    MEMGRAPH_HOST  (default: localhost)
    MEMGRAPH_PORT  (default: 7687)

Run:
    pytest tests/test_clustering.py -v

═══════════════════════════════════════════════════════════════════════════════
Fixture topologies
═══════════════════════════════════════════════════════════════════════════════

isolated_fraud_cluster  (cluster_id=1, 30 nodes, status='fraud')
─────────────────────────────────────────────────────────────────
30-node ring, all edges internal, weight=1.

  iso_00 ──(1)──► iso_01 ──(1)──► ... ──(1)──► iso_29 ──(1)──► iso_00

  internal_weight      = 30  (30 ring edges × weight=1; each traversed
                               twice in undirected MATCH → raw 60 / 2)
  external_weight      = 0
  closedness           = 1.0
  avg_internal_weight  = 1.0  (30 edges / 30 edges)
  fraud_ratio          = 1.0


semi_open_cluster  (cluster_id=2, 30 nodes, status='pending')
──────────────────────────────────────────────────────────────
30-node ring (internal) + 13 directed edges to bridge nodes (cluster_id=99).

  semi_00 ──► semi_01 ──► ... ──► semi_29 ──► semi_00    (ring, w=1)
  semi_00 ──► bridge_00  |
  semi_01 ──► bridge_01  |  13 external edges (w=1)
  ...                    |
  semi_12 ──► bridge_12  |

  internal_weight  = 30
  external_weight  = 13
  closedness       = 30 / 43 ≈ 0.6977


normal_applications  (cluster_id=3, 100 nodes, status='approved')
──────────────────────────────────────────────────────────────────
100-node chain (internal) + 3 external edges per node to 10 hub nodes
(cluster_id=98).  Hub j receives edges from norm_i where i % 10 == j,
(i+3) % 10 == j, or (i+6) % 10 == j.

  norm_00 ──► norm_01 ──► ... ──► norm_99    (chain, w=1)
  norm_i  ──► hub_{i%10}          ⎫
  norm_i  ──► hub_{(i+3)%10}      ⎬  300 external edges total (w=1)
  norm_i  ──► hub_{(i+6)%10}      ⎭

  internal_weight  = 99
  external_weight  = 300
  closedness       = 99 / 399 ≈ 0.2481
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from neo4j import GraphDatabase
except ImportError:
    pytest.skip("neo4j driver required: pip install neo4j", allow_module_level=True)

try:
    from graph_client import FraudGraphClient
except ImportError:
    pytest.skip("graph_client not found in project root", allow_module_level=True)

# ── paths ─────────────────────────────────────────────────────────────────────

ROOT          = Path(__file__).resolve().parent.parent
SCHEMA_FILE   = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "schema" / "triggers.cypher"

# ── pre-computed expected values ──────────────────────────────────────────────

_ISO_CLUSTER_ID       = 1
_ISO_SIZE             = 30
_ISO_CLOSEDNESS       = 1.0          # fully isolated ring
_ISO_AVG_INT_WEIGHT   = 1.0          # 30 ring edges × weight 1 / 30
_ISO_FRAUD_RATIO      = 1.0

_SEMI_CLUSTER_ID      = 2
_SEMI_CLOSEDNESS      = 30.0 / 43.0  # ≈ 0.6977

_NORM_CLUSTER_ID      = 3
_NORM_CLOSEDNESS      = 99.0 / 399.0 # ≈ 0.2481

# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_cypher(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if stripped.startswith("//"):
            continue
        idx = raw.find("//")
        if idx != -1:
            raw = raw[:idx]
        lines.append(raw)
    text = "\n".join(lines)
    return [s.strip() for s in text.split(";") if s.strip()]


def _run(session, query: str, **kwargs) -> None:
    """Execute a Cypher statement, consuming the result."""
    session.run(query, **kwargs).consume()


def _batch_create_nodes(session, rows: list[dict]) -> None:
    """Create Application nodes in one UNWIND transaction."""
    _run(
        session,
        """
        UNWIND $rows AS row
        CREATE (a:Application)
        SET a.id         = row.id,
            a.cluster_id = row.cluster_id,
            a.status     = row.status,
            a.phone      = row.phone,
            a.email      = row.email,
            a.document   = row.document,
            a.amount     = row.amount,
            a.created_at = localDateTime(row.ts)
        """,
        rows=rows,
    )


def _batch_create_edges(session, pairs: list[dict]) -> None:
    """Create LINKED edges in one UNWIND transaction.

    Each dict in *pairs* must have keys: src, dst, w.
    """
    _run(
        session,
        """
        UNWIND $pairs AS p
        MATCH (a:Application {id: p.src}), (b:Application {id: p.dst})
        CREATE (a)-[:LINKED {weight: p.w, shared_attrs: ['phone']}]->(b)
        """,
        pairs=pairs,
    )


# ── session-scoped fixtures ───────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mg_driver():
    """Session-scoped Bolt driver; skips the entire suite if Memgraph is down."""
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=("", ""))
    try:
        with driver.session() as s:
            s.run("RETURN 1").consume()
    except Exception as exc:
        driver.close()
        pytest.skip(f"Memgraph unavailable at {host}:{port}: {exc}")
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def client(mg_driver):
    """Session-scoped FraudGraphClient."""
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    c = FraudGraphClient(host=host, port=port)
    yield c
    c.close()


@pytest.fixture(scope="session", autouse=True)
def mg_setup(mg_driver):
    """Apply schema.cypher + triggers.cypher once for the whole test session."""
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
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
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
        try:
            s.run("DROP TRIGGER link_new_applications").consume()
        except Exception:
            pass


@pytest.fixture(scope="session")
def requires_mage(client):
    """Skip any test that requires the MAGE community_detection module.

    Checked once per session; if the procedure is missing all dependent tests
    are skipped with a clear message rather than failing with a Cypher error.
    """
    result = client.execute_raw(
        "CALL mg.procedures() YIELD name "
        "WHERE name = 'community_detection.get_subgraph' RETURN name"
    )
    if not result:
        pytest.skip(
            "MAGE community_detection.get_subgraph not available — "
            "install MAGE or point at a MAGE-enabled Memgraph instance"
        )


# ── function-scoped fixtures ──────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mg_clean(mg_driver):
    """Wipe all nodes and edges before every test for full isolation."""
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
    yield


# ── data fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture()
def isolated_fraud_cluster(mg_driver):
    """Create 30 Application nodes in a closed ring (cluster_id=1, all fraud).

    Topology: iso_00 → iso_01 → ... → iso_29 → iso_00 (ring, weight=1)
    Zero external edges.

    Expected metrics:
        closedness           = 1.0
        avg_internal_weight  = 1.0
        fraud_ratio          = 1.0
        size                 = 30
    """
    n = _ISO_SIZE
    nodes = [
        {
            "id":         f"iso_{i:02d}",
            "cluster_id": _ISO_CLUSTER_ID,
            "status":     "fraud",
            "phone":      f"iso_ph_{i:04d}",
            "email":      f"iso_{i:04d}@fraud.test",
            "document":   f"ISODOC{i:04d}",
            "amount":     float(10_000 + i * 500),
            "ts":         f"2024-01-{i + 1:02d}T10:00:00",
        }
        for i in range(n)
    ]
    ring_edges = [
        {"src": f"iso_{i:02d}", "dst": f"iso_{(i + 1) % n:02d}", "w": 1}
        for i in range(n)
    ]
    with mg_driver.session() as s:
        _batch_create_nodes(s, nodes)
        _batch_create_edges(s, ring_edges)
    return {"cluster_id": _ISO_CLUSTER_ID, "ids": [f"iso_{i:02d}" for i in range(n)]}


@pytest.fixture()
def semi_open_cluster(mg_driver):
    """Create a 30-node ring (cluster_id=2) with 13 cross-cluster edges.

    Internal: 30 ring edges, weight=1.
    External: semi_00..12 each connect to a dedicated bridge node
              (cluster_id=99), weight=1.

    Expected metrics:
        closedness  = 30 / 43 ≈ 0.6977
        size        = 30
    """
    n      = 30
    n_ext  = 13   # number of cross-cluster edges
    cid    = _SEMI_CLUSTER_ID
    bridge_cid = 99

    semi_nodes = [
        {
            "id":         f"semi_{i:02d}",
            "cluster_id": cid,
            "status":     "pending",
            "phone":      f"semi_ph_{i:04d}",
            "email":      f"semi_{i:04d}@semi.test",
            "document":   f"SEMIDOC{i:04d}",
            "amount":     float(5_000 + i * 100),
            "ts":         f"2024-02-{i + 1:02d}T10:00:00",
        }
        for i in range(n)
    ]
    bridge_nodes = [
        {
            "id":         f"bridge_{i:02d}",
            "cluster_id": bridge_cid,
            "status":     "pending",
            "phone":      f"bridge_ph_{i:04d}",
            "email":      f"bridge_{i:04d}@bridge.test",
            "document":   f"BRDOC{i:04d}",
            "amount":     1_000.0,
            "ts":         "2024-02-15T10:00:00",
        }
        for i in range(n_ext)
    ]
    ring_edges = [
        {"src": f"semi_{i:02d}", "dst": f"semi_{(i + 1) % n:02d}", "w": 1}
        for i in range(n)
    ]
    cross_edges = [
        {"src": f"semi_{i:02d}", "dst": f"bridge_{i:02d}", "w": 1}
        for i in range(n_ext)
    ]
    with mg_driver.session() as s:
        _batch_create_nodes(s, semi_nodes + bridge_nodes)
        _batch_create_edges(s, ring_edges + cross_edges)
    return {"cluster_id": cid, "ids": [f"semi_{i:02d}" for i in range(n)]}


@pytest.fixture()
def normal_applications(mg_driver):
    """Create 100 Application nodes (cluster_id=3) with high external connectivity.

    Internal: 99-node chain, weight=1.
    External: each norm_i connects to 3 hub nodes (cluster_id=98)
              hub_{i%10}, hub_{(i+3)%10}, hub_{(i+6)%10} — weight=1.
              Total: 100 × 3 = 300 external edges.

    Expected metrics:
        closedness  = 99 / 399 ≈ 0.2481  (< 0.3)
        size        = 100
    """
    n      = 100
    n_hubs = 10
    cid    = _NORM_CLUSTER_ID
    hub_cid = 98

    norm_nodes = [
        {
            "id":         f"norm_{i:03d}",
            "cluster_id": cid,
            "status":     "approved",
            "phone":      f"norm_ph_{i:04d}",
            "email":      f"norm_{i:04d}@normal.test",
            "document":   f"NORMDOC{i:04d}",
            "amount":     float(8_000 + i * 50),
            "ts":         f"2024-03-{(i % 28) + 1:02d}T{(i % 24):02d}:00:00",
        }
        for i in range(n)
    ]
    hub_nodes = [
        {
            "id":         f"hub_{j:02d}",
            "cluster_id": hub_cid,
            "status":     "approved",
            "phone":      f"hub_ph_{j:04d}",
            "email":      f"hub_{j:04d}@hub.test",
            "document":   f"HUBDOC{j:04d}",
            "amount":     2_000.0,
            "ts":         "2024-03-15T12:00:00",
        }
        for j in range(n_hubs)
    ]
    chain_edges = [
        {"src": f"norm_{i:03d}", "dst": f"norm_{i + 1:03d}", "w": 1}
        for i in range(n - 1)
    ]
    hub_edges = [
        {"src": f"norm_{i:03d}", "dst": f"hub_{(i + offset) % n_hubs:02d}", "w": 1}
        for i in range(n)
        for offset in (0, 3, 6)
    ]
    with mg_driver.session() as s:
        _batch_create_nodes(s, norm_nodes + hub_nodes)
        _batch_create_edges(s, chain_edges + hub_edges)
    return {"cluster_id": cid, "ids": [f"norm_{i:03d}" for i in range(n)]}


# ── tests ─────────────────────────────────────────────────────────────────────

def test_louvain_finds_isolated_cluster(client, mg_driver, requires_mage):
    """Louvain must assign all 30 ring nodes the same community_id.

    Two separate connected components are created so that Louvain has a
    meaningful graph structure to partition (at least 2 communities):
      • louvain_ring_00..29 — closed 30-node ring, no external edges
      • louvain_noise_00..04 — 5-node star, fully disconnected from the ring

    After running community_detection.get_subgraph the test verifies that every
    louvain_ring_* node received the same cluster_id (i.e., Louvain placed the
    entire ring in one community).
    """
    n_ring  = 30
    n_noise = 5

    ring_nodes = [
        {
            "id":         f"louvain_ring_{i:02d}",
            "cluster_id": None,          # Louvain will assign
            "status":     "fraud",
            "phone":      f"lvring_ph_{i:04d}",
            "email":      f"lvring_{i:04d}@louvain.test",
            "document":   f"LVRINGDOC{i:04d}",
            "amount":     1_000.0,
            "ts":         "2024-05-01T10:00:00",
        }
        for i in range(n_ring)
    ]
    noise_nodes = [
        {
            "id":         f"louvain_noise_{i:02d}",
            "cluster_id": None,
            "status":     "pending",
            "phone":      f"lvnoise_ph_{i:04d}",
            "email":      f"lvnoise_{i:04d}@louvain.test",
            "document":   f"LVNOISEDOC{i:04d}",
            "amount":     500.0,
            "ts":         "2024-05-01T10:00:00",
        }
        for i in range(n_noise)
    ]

    # Ring: louvain_ring_00 → ... → louvain_ring_29 → louvain_ring_00
    ring_edges = [
        {"src": f"louvain_ring_{i:02d}", "dst": f"louvain_ring_{(i + 1) % n_ring:02d}", "w": 2}
        for i in range(n_ring)
    ]
    # Noise: hub (louvain_noise_00) connected to 4 satellites
    noise_edges = [
        {"src": "louvain_noise_00", "dst": f"louvain_noise_{i:02d}", "w": 1}
        for i in range(1, n_noise)
    ]

    with mg_driver.session() as s:
        _batch_create_nodes(s, ring_nodes + noise_nodes)
        _batch_create_edges(s, ring_edges + noise_edges)

    # Run Louvain on the full Application-LINKED subgraph
    client.execute_raw("""
        MATCH (n:Application)-[r:LINKED]-(m:Application)
        WITH collect(DISTINCT n) AS nodes, collect(DISTINCT r) AS rels
        CALL community_detection.get_subgraph(nodes, rels, false, true, "weight")
        YIELD node, community_id
        SET node.cluster_id = community_id
    """)

    # All ring nodes must share a single community_id
    rows = client.execute_raw(
        "MATCH (a:Application) WHERE a.id STARTS WITH 'louvain_ring_' "
        "RETURN collect(DISTINCT a.cluster_id) AS cids"
    )
    assigned_cids = rows[0]["cids"]
    assert len(assigned_cids) == 1, (
        f"Expected all 30 ring nodes in one community, "
        f"got {len(assigned_cids)} distinct cluster_ids: {assigned_cids}"
    )
    assert assigned_cids[0] is not None, "Louvain did not assign cluster_id to ring nodes"


def test_closedness_isolated(client, isolated_fraud_cluster):
    """A ring with zero external edges must have closedness = 1.0 (> 0.95)."""
    cid    = isolated_fraud_cluster["cluster_id"]
    result = client.calculate_cluster_closedness(cid)

    assert isinstance(result, float)
    assert result > 0.95, f"Expected closedness > 0.95, got {result}"
    assert result == pytest.approx(_ISO_CLOSEDNESS)


def test_closedness_semi_open(client, semi_open_cluster):
    """A ring with 13 external edges out of 43 total must have closedness ≈ 0.70.

    Exact expected value: 30 / 43 ≈ 0.6977.
    Tolerance: ±0.05 (accounts for floating-point rounding in Memgraph).
    """
    cid    = semi_open_cluster["cluster_id"]
    result = client.calculate_cluster_closedness(cid)

    assert isinstance(result, float)
    assert result == pytest.approx(_SEMI_CLOSEDNESS, abs=0.05), (
        f"Expected closedness ≈ {_SEMI_CLOSEDNESS:.4f} (30/43), got {result:.4f}"
    )
    # Confirm it is in the "semi-open" band — neither isolated nor fully open
    assert 0.6 < result < 0.8, f"Expected closedness in (0.6, 0.8), got {result}"


def test_closedness_normal(client, normal_applications):
    """Normal applications with high external connectivity must have closedness < 0.3.

    Exact expected value: 99 / 399 ≈ 0.2481.
    """
    cid    = normal_applications["cluster_id"]
    result = client.calculate_cluster_closedness(cid)

    assert isinstance(result, float)
    assert result < 0.3, f"Expected closedness < 0.3, got {result}"
    assert result == pytest.approx(_NORM_CLOSEDNESS, abs=0.05), (
        f"Expected closedness ≈ {_NORM_CLOSEDNESS:.4f} (99/399), got {result:.4f}"
    )


def test_get_suspicious_clusters(client, isolated_fraud_cluster):
    """The isolated fraud cluster (closedness=1.0, size=30) must appear in
    the suspicious clusters list when thresholds are set to 0.7 / 25.
    """
    results = client.get_suspicious_clusters(
        closedness_threshold=0.7,
        min_size=25,
    )

    assert isinstance(results, list)
    assert len(results) >= 1, "Expected at least one suspicious cluster"

    flagged_ids = [r["cluster_id"] for r in results]
    assert _ISO_CLUSTER_ID in flagged_ids, (
        f"Isolated fraud cluster (id={_ISO_CLUSTER_ID}) not in suspicious list: "
        f"{flagged_ids}"
    )

    # The isolated cluster should rank first (highest closedness)
    top = results[0]
    assert top["cluster_id"] == _ISO_CLUSTER_ID
    assert top["closedness"] > 0.7
    assert top["size"] == _ISO_SIZE
    assert top["fraud_ratio"] == pytest.approx(_ISO_FRAUD_RATIO)

    # Results must be sorted by closedness DESC
    closedness_vals = [r["closedness"] for r in results]
    assert closedness_vals == sorted(closedness_vals, reverse=True)


def test_incremental_assignment(client, isolated_fraud_cluster, mg_driver):
    """A new application connected to isolated_fraud_cluster gets its cluster_id.

    Setup: 'newcomer' is linked to three cluster-1 nodes with different weights.
    Weighted majority vote: cluster_id=1 wins (total weight = 2+1+1 = 4 vs. none).
    """
    # Create newcomer without cluster_id (unique attributes, no trigger links)
    with mg_driver.session() as s:
        _run(
            s,
            "CREATE (a:Application) "
            "SET a.id = 'newcomer', a.status = 'pending', "
            "    a.phone = 'newcomer_ph_9999', "
            "    a.email = 'newcomer@assign.test', "
            "    a.document = 'NEWCOMERDOC', "
            "    a.amount = 15000.0, "
            "    a.created_at = localDateTime('2024-06-01T00:00:00')",
        )
        # Manual LINKED edges to three cluster-1 nodes with varying weights
        _run(
            s,
            "MATCH (a:Application {id: 'newcomer'}), (b:Application {id: 'iso_00'}) "
            "CREATE (a)-[:LINKED {weight: 2, shared_attrs: ['phone']}]->(b)",
        )
        _run(
            s,
            "MATCH (a:Application {id: 'newcomer'}), (b:Application {id: 'iso_01'}) "
            "CREATE (a)-[:LINKED {weight: 1, shared_attrs: ['document']}]->(b)",
        )
        _run(
            s,
            "MATCH (a:Application {id: 'newcomer'}), (b:Application {id: 'iso_02'}) "
            "CREATE (a)-[:LINKED {weight: 1, shared_attrs: ['email']}]->(b)",
        )

    assigned = client.assign_new_application_to_cluster("newcomer")

    assert isinstance(assigned, int)
    assert assigned == _ISO_CLUSTER_ID, (
        f"Expected newcomer to join cluster {_ISO_CLUSTER_ID}, got {assigned}"
    )

    # Verify the property is actually written to the node in Memgraph
    with mg_driver.session() as s:
        rec = s.run(
            "MATCH (a:Application {id: 'newcomer'}) RETURN a.cluster_id AS cid"
        ).single()
    assert rec["cid"] == _ISO_CLUSTER_ID


def test_cluster_stats_accuracy(client, isolated_fraud_cluster):
    """get_cluster_stats must return exact values for the isolated_fraud_cluster.

    All fields are verified against the topology defined in the fixture:
      • size = 30 (30 Application nodes)
      • closedness = 1.0 (ring, no external edges)
      • avg_internal_weight = 1.0 (30 edges of weight 1 / 30 edge count)
      • fraud_ratio = 1.0 (all nodes have status='fraud')
      • oldest_app < newest_app (created_at spans 2024-01-01 to 2024-01-30)
    """
    cid   = isolated_fraud_cluster["cluster_id"]
    stats = client.get_cluster_stats(cid)

    # Shape
    assert isinstance(stats, dict)
    required_keys = {
        "cluster_id", "size", "closedness",
        "avg_internal_weight", "fraud_ratio",
        "oldest_app", "newest_app",
    }
    assert required_keys == stats.keys(), (
        f"Missing keys: {required_keys - stats.keys()}"
    )

    # Scalar fields
    assert stats["cluster_id"] == _ISO_CLUSTER_ID
    assert stats["size"]        == _ISO_SIZE

    assert stats["closedness"]  == pytest.approx(_ISO_CLOSEDNESS), (
        f"Expected closedness={_ISO_CLOSEDNESS}, got {stats['closedness']}"
    )
    assert stats["avg_internal_weight"] == pytest.approx(_ISO_AVG_INT_WEIGHT), (
        f"Expected avg_internal_weight={_ISO_AVG_INT_WEIGHT}, "
        f"got {stats['avg_internal_weight']}"
    )
    assert stats["fraud_ratio"] == pytest.approx(_ISO_FRAUD_RATIO), (
        f"Expected fraud_ratio={_ISO_FRAUD_RATIO}, got {stats['fraud_ratio']}"
    )

    # Temporal fields
    assert stats["oldest_app"] is not None, "oldest_app must not be None"
    assert stats["newest_app"] is not None, "newest_app must not be None"
    assert stats["oldest_app"] < stats["newest_app"], (
        "oldest_app must be strictly earlier than newest_app "
        f"({stats['oldest_app']} vs {stats['newest_app']})"
    )

    # Value invariants
    assert 0.0 <= stats["closedness"]  <= 1.0
    assert 0.0 <= stats["fraud_ratio"] <= 1.0
    assert stats["avg_internal_weight"] >= 0.0
    assert stats["size"] > 0
