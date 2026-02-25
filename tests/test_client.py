"""Tests for FraudGraphClient cluster-metric methods.

Requires:
    - Running Memgraph instance (bolt://localhost:7687 by default)
    - pip install neo4j pytest

Configure via env vars:
    MEMGRAPH_HOST  (default: localhost)
    MEMGRAPH_PORT  (default: 7687)

Run:
    pytest tests/test_client.py -v

Test topology (recreated before every test by `cluster_topology` fixture):

    Cluster 1 — fraud ring (3 nodes, 2 internal edges, 1 external edge)

        c1_a ──(w=3)──► c1_b ──(w=2)──► c1_c
         │                                      cluster_id=1
         └──(w=1)──► c2_a               cluster_id=2

    Expected metrics for cluster 1:
        internal_weight      = (3+2) = 5.0   (each edge counted from both
                                               sides → raw sum / 2)
        external_weight      = 1.0            (edge to c2_a, counted once
                                               from c1_a side)
        closedness           = 5/6 ≈ 0.8333
        avg_internal_weight  = 5/2 = 2.5     (2 internal edges)
        fraud_ratio          = 1.0            (all three nodes are 'fraud')
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Allow importing graph_client from the project root
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

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "triggers.cypher"


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


# Exact expected values derived from the topology diagram above.
_CLUSTER_1_CLOSEDNESS       = 5.0 / 6.0   # ≈ 0.8333
_CLUSTER_1_AVG_INT_WEIGHT   = 2.5
_CLUSTER_1_SIZE             = 3
_CLUSTER_1_FRAUD_RATIO      = 1.0
_CLUSTER_2_CLOSEDNESS       = 0.0          # c2_a has one external edge only
_CLUSTER_2_SIZE             = 1


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


# ── function-scoped fixtures ──────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mg_clean(mg_driver):
    """Wipe all nodes and edges before every test for full isolation."""
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
    yield


@pytest.fixture()
def cluster_topology(mg_driver):
    """Create the canonical 2-cluster test topology (see module docstring).

    All nodes are inserted via direct CREATE with pre-set cluster_id so that
    closedness expectations are deterministic and independent of Louvain.
    LINKED edges are created manually with explicit weights so no attribute
    sharing is needed — the trigger fires on each CREATE but finds no matches
    (all attributes are unique), so no duplicate edges are created.
    """
    with mg_driver.session() as s:
        # Cluster 1 — fraud ring
        s.run("""
            CREATE (:Application {
                id: 'c1_a', cluster_id: 1, status: 'fraud',
                phone: 'phc1a', email: 'c1a@test.com', document: 'D001',
                amount: 10000.0, created_at: localDateTime('2024-01-01T10:00:00')
            })
        """).consume()
        s.run("""
            CREATE (:Application {
                id: 'c1_b', cluster_id: 1, status: 'fraud',
                phone: 'phc1b', email: 'c1b@test.com', document: 'D002',
                amount: 20000.0, created_at: localDateTime('2024-02-01T10:00:00')
            })
        """).consume()
        s.run("""
            CREATE (:Application {
                id: 'c1_c', cluster_id: 1, status: 'fraud',
                phone: 'phc1c', email: 'c1c@test.com', document: 'D003',
                amount: 30000.0, created_at: localDateTime('2024-03-01T10:00:00')
            })
        """).consume()

        # Cluster 2 — benign singleton
        s.run("""
            CREATE (:Application {
                id: 'c2_a', cluster_id: 2, status: 'pending',
                phone: 'phc2a', email: 'c2a@test.com', document: 'D004',
                amount: 5000.0, created_at: localDateTime('2024-04-01T10:00:00')
            })
        """).consume()

        # Intra-cluster edges (cluster 1)
        s.run("""
            MATCH (a:Application {id: 'c1_a'}), (b:Application {id: 'c1_b'})
            CREATE (a)-[:LINKED {weight: 3, shared_attrs: ['phone','email','document']}]->(b)
        """).consume()
        s.run("""
            MATCH (a:Application {id: 'c1_b'}), (b:Application {id: 'c1_c'})
            CREATE (a)-[:LINKED {weight: 2, shared_attrs: ['phone','email']}]->(b)
        """).consume()

        # Cross-cluster edge (cluster 1 → cluster 2)
        s.run("""
            MATCH (a:Application {id: 'c1_a'}), (b:Application {id: 'c2_a'})
            CREATE (a)-[:LINKED {weight: 1, shared_attrs: ['phone']}]->(b)
        """).consume()

    yield {"c1": ["c1_a", "c1_b", "c1_c"], "c2": ["c2_a"]}


# ── 1. calculate_cluster_closedness ──────────────────────────────────────────

class TestCalculateClusterClosedness:

    def test_closed_cluster_value(self, client, cluster_topology):
        """Cluster 1 should have closedness = 5/6 ≈ 0.8333."""
        result = client.calculate_cluster_closedness(1)
        assert isinstance(result, float)
        assert result == pytest.approx(_CLUSTER_1_CLOSEDNESS, rel=1e-4)

    def test_open_cluster_value(self, client, cluster_topology):
        """Cluster 2 has only an incoming cross-cluster edge — closedness = 0.0."""
        result = client.calculate_cluster_closedness(2)
        assert result == pytest.approx(_CLUSTER_2_CLOSEDNESS)

    def test_nonexistent_cluster_returns_zero(self, client, cluster_topology):
        """Unknown cluster_id must return 0.0, not raise."""
        assert client.calculate_cluster_closedness(999) == 0.0

    def test_return_type_is_float(self, client, cluster_topology):
        assert type(client.calculate_cluster_closedness(1)) is float

    def test_value_in_unit_interval(self, client, cluster_topology):
        v = client.calculate_cluster_closedness(1)
        assert 0.0 <= v <= 1.0


# ── 2. get_cluster_stats ──────────────────────────────────────────────────────

class TestGetClusterStats:

    def test_returns_dict_with_all_keys(self, client, cluster_topology):
        stats = client.get_cluster_stats(1)
        assert isinstance(stats, dict)
        expected_keys = {
            "cluster_id", "size", "closedness",
            "avg_internal_weight", "fraud_ratio",
            "oldest_app", "newest_app",
        }
        assert expected_keys == stats.keys()

    def test_cluster_id_echoed(self, client, cluster_topology):
        assert client.get_cluster_stats(1)["cluster_id"] == 1

    def test_size(self, client, cluster_topology):
        assert client.get_cluster_stats(1)["size"] == _CLUSTER_1_SIZE

    def test_closedness(self, client, cluster_topology):
        stats = client.get_cluster_stats(1)
        assert stats["closedness"] == pytest.approx(_CLUSTER_1_CLOSEDNESS, rel=1e-4)

    def test_avg_internal_weight(self, client, cluster_topology):
        stats = client.get_cluster_stats(1)
        assert stats["avg_internal_weight"] == pytest.approx(_CLUSTER_1_AVG_INT_WEIGHT)

    def test_fraud_ratio_all_fraud(self, client, cluster_topology):
        stats = client.get_cluster_stats(1)
        assert stats["fraud_ratio"] == pytest.approx(_CLUSTER_1_FRAUD_RATIO)

    def test_fraud_ratio_no_fraud(self, client, cluster_topology):
        stats = client.get_cluster_stats(2)
        assert stats["fraud_ratio"] == pytest.approx(0.0)

    def test_oldest_and_newest_app_order(self, client, cluster_topology):
        stats = client.get_cluster_stats(1)
        assert stats["oldest_app"] is not None
        assert stats["newest_app"] is not None
        assert stats["oldest_app"] <= stats["newest_app"]

    def test_isolated_cluster_returns_zeros_for_edge_stats(self, client, mg_driver):
        """A cluster with no LINKED edges should return closedness=0, avg_iw=0."""
        with mg_driver.session() as s:
            s.run("""
                CREATE (:Application {
                    id: 'iso', cluster_id: 99, status: 'pending',
                    phone: 'phiso', email: 'iso@t.com', document: 'D099',
                    amount: 1.0, created_at: localDateTime('2024-01-01T00:00:00')
                })
            """).consume()
        stats = client.get_cluster_stats(99)
        assert stats["size"] == 1
        assert stats["closedness"] == pytest.approx(0.0)
        assert stats["avg_internal_weight"] == pytest.approx(0.0)

    def test_nonexistent_cluster_returns_empty_dict(self, client, cluster_topology):
        assert client.get_cluster_stats(999) == {}


# ── 3. get_all_cluster_stats ──────────────────────────────────────────────────

class TestGetAllClusterStats:

    def test_returns_generator(self, client, cluster_topology):
        import types
        result = client.get_all_cluster_stats(min_size=1)
        assert isinstance(result, types.GeneratorType)

    def test_min_size_1_returns_both_clusters(self, client, cluster_topology):
        rows = list(client.get_all_cluster_stats(min_size=1))
        cluster_ids = {r["cluster_id"] for r in rows}
        assert cluster_ids == {1, 2}

    def test_min_size_filters_small_clusters(self, client, cluster_topology):
        rows = list(client.get_all_cluster_stats(min_size=2))
        cluster_ids = {r["cluster_id"] for r in rows}
        assert 1 in cluster_ids
        assert 2 not in cluster_ids  # cluster 2 has only 1 node

    def test_each_row_has_required_keys(self, client, cluster_topology):
        required = {
            "cluster_id", "size", "closedness",
            "avg_internal_weight", "fraud_ratio",
            "oldest_app", "newest_app",
        }
        for row in client.get_all_cluster_stats(min_size=1):
            assert required == row.keys()

    def test_sorted_by_closedness_desc(self, client, cluster_topology):
        rows = list(client.get_all_cluster_stats(min_size=1))
        closedness_values = [r["closedness"] for r in rows]
        assert closedness_values == sorted(closedness_values, reverse=True)

    def test_cluster_1_stats_match_expected(self, client, cluster_topology):
        rows = {r["cluster_id"]: r for r in client.get_all_cluster_stats(min_size=1)}
        c1 = rows[1]
        assert c1["size"]       == _CLUSTER_1_SIZE
        assert c1["closedness"] == pytest.approx(_CLUSTER_1_CLOSEDNESS, rel=1e-4)
        assert c1["fraud_ratio"]== pytest.approx(_CLUSTER_1_FRAUD_RATIO)

    def test_empty_graph_yields_nothing(self, client):
        # mg_clean already wiped the graph; no cluster_topology here
        rows = list(client.get_all_cluster_stats(min_size=1))
        assert rows == []

    def test_fraud_ratio_values_in_unit_interval(self, client, cluster_topology):
        for row in client.get_all_cluster_stats(min_size=1):
            assert 0.0 <= row["fraud_ratio"] <= 1.0

    def test_closedness_values_in_unit_interval(self, client, cluster_topology):
        for row in client.get_all_cluster_stats(min_size=1):
            assert 0.0 <= row["closedness"] <= 1.0


# ── 4. get_suspicious_clusters ────────────────────────────────────────────────

class TestGetSuspiciousClusters:

    def test_returns_list(self, client, cluster_topology):
        result = client.get_suspicious_clusters(closedness_threshold=0.7, min_size=2)
        assert isinstance(result, list)

    def test_cluster_1_flagged_with_relaxed_thresholds(self, client, cluster_topology):
        result = client.get_suspicious_clusters(closedness_threshold=0.7, min_size=2)
        ids = [r["cluster_id"] for r in result]
        assert 1 in ids

    def test_cluster_2_not_flagged(self, client, cluster_topology):
        """Cluster 2 has closedness=0.0, well below the threshold."""
        result = client.get_suspicious_clusters(closedness_threshold=0.7, min_size=1)
        ids = [r["cluster_id"] for r in result]
        assert 2 not in ids

    def test_high_threshold_excludes_everything(self, client, cluster_topology):
        result = client.get_suspicious_clusters(closedness_threshold=0.99, min_size=1)
        assert result == []

    def test_min_size_excludes_small_clusters(self, client, cluster_topology):
        # cluster 1 has size=3; min_size=10 excludes it
        result = client.get_suspicious_clusters(closedness_threshold=0.0, min_size=10)
        assert result == []

    def test_sorted_by_closedness_desc(self, client, cluster_topology):
        result = client.get_suspicious_clusters(closedness_threshold=0.0, min_size=1)
        closedness_values = [r["closedness"] for r in result]
        assert closedness_values == sorted(closedness_values, reverse=True)

    def test_result_row_has_required_keys(self, client, cluster_topology):
        required = {
            "cluster_id", "size", "closedness",
            "avg_internal_weight", "fraud_ratio",
            "oldest_app", "newest_app",
        }
        result = client.get_suspicious_clusters(closedness_threshold=0.0, min_size=1)
        for row in result:
            assert required == row.keys()

    def test_closedness_exceeds_threshold_for_all_results(self, client, cluster_topology):
        threshold = 0.7
        result = client.get_suspicious_clusters(
            closedness_threshold=threshold, min_size=1
        )
        for row in result:
            assert row["closedness"] > threshold

    def test_empty_when_no_data(self, client):
        result = client.get_suspicious_clusters(closedness_threshold=0.0, min_size=1)
        assert result == []


# ── 5. assign_new_application_to_cluster ──────────────────────────────────────

class TestAssignNewApplicationToCluster:

    def _create_app(self, mg_driver, app_id: str, **extra):
        with mg_driver.session() as s:
            props = {
                "id": app_id,
                "phone": f"ph_{app_id}",
                "email": f"{app_id}@test.com",
                "document": f"DOC_{app_id}",
                "amount": 1000.0,
                "status": "pending",
                **extra,
            }
            s.run(
                "CREATE (a:Application) SET a += $props, "
                "a.created_at = localDateTime('2024-06-01T00:00:00')",
                props=props,
            ).consume()

    def _link(self, mg_driver, src_id: str, dst_id: str, weight: int):
        with mg_driver.session() as s:
            s.run(
                "MATCH (a:Application {id: $src}), (b:Application {id: $dst}) "
                "CREATE (a)-[:LINKED {weight: $w, shared_attrs: ['phone']}]->(b)",
                src=src_id, dst=dst_id, w=weight,
            ).consume()

    def test_majority_vote_assigns_correct_cluster(self, client, cluster_topology, mg_driver):
        """New app linked to cluster-1 nodes (w=2) AND cluster-2 node (w=1) → cluster 1."""
        self._create_app(mg_driver, "new_e")
        self._link(mg_driver, "new_e", "c1_a", weight=2)  # votes for cluster 1
        self._link(mg_driver, "new_e", "c2_a", weight=1)  # votes for cluster 2

        assigned = client.assign_new_application_to_cluster("new_e")
        assert assigned == 1

    def test_single_neighbour_determines_cluster(self, client, cluster_topology, mg_driver):
        """One neighbour in cluster 1 → cluster 1."""
        self._create_app(mg_driver, "new_f")
        self._link(mg_driver, "new_f", "c1_b", weight=3)

        assigned = client.assign_new_application_to_cluster("new_f")
        assert assigned == 1

    def test_no_neighbours_creates_new_cluster(self, client, cluster_topology, mg_driver):
        """Isolated app gets max_cluster_id + 1 = 3."""
        self._create_app(mg_driver, "new_g")

        assigned = client.assign_new_application_to_cluster("new_g")
        assert assigned == 3  # max existing = 2 → 2 + 1

    def test_result_is_int(self, client, cluster_topology, mg_driver):
        self._create_app(mg_driver, "new_h")
        self._link(mg_driver, "new_h", "c1_a", weight=1)
        result = client.assign_new_application_to_cluster("new_h")
        assert isinstance(result, int)

    def test_cluster_id_written_to_node(self, client, cluster_topology, mg_driver):
        """After assignment, the node's cluster_id property must be updated."""
        self._create_app(mg_driver, "new_i")
        self._link(mg_driver, "new_i", "c1_c", weight=5)

        assigned = client.assign_new_application_to_cluster("new_i")

        with mg_driver.session() as s:
            rec = s.run(
                "MATCH (a:Application {id: 'new_i'}) RETURN a.cluster_id AS cid"
            ).single()
        assert rec["cid"] == assigned

    def test_nonexistent_app_raises_value_error(self, client):
        with pytest.raises(ValueError, match="ghost_app"):
            client.assign_new_application_to_cluster("ghost_app")

    def test_first_app_in_empty_graph_gets_cluster_1(self, client, mg_driver):
        """If the graph is empty (no existing cluster_ids), fallback assigns 1."""
        # mg_clean wiped everything; no cluster_topology here
        self._create_app(mg_driver, "first_ever")
        assigned = client.assign_new_application_to_cluster("first_ever")
        assert assigned == 1
