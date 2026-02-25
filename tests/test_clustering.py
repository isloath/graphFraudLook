"""Integration tests for clustering and cluster metrics.

Requires:
- running Memgraph (bolt://localhost:7687)
- neo4j python driver
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

try:
    from neo4j import GraphDatabase
except ImportError:
    pytest.skip("neo4j driver required: pip install neo4j", allow_module_level=True)

from graph_client import FraudGraphClient

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "triggers.cypher"


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


@pytest.fixture(scope="session")
def mg_driver():
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


@pytest.fixture(scope="session", autouse=True)
def mg_schema(mg_driver):
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


@pytest.fixture(autouse=True)
def mg_clean(mg_driver):
    with mg_driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
    yield


@pytest.fixture
def client() -> FraudGraphClient:
    host = os.environ.get("MEMGRAPH_HOST", "localhost")
    port = int(os.environ.get("MEMGRAPH_PORT", "7687"))
    c = FraudGraphClient(host=host, port=port, pool_size=5)
    yield c
    c.close()


def _create_app(
    client: FraudGraphClient,
    app_id: str,
    phone: str,
    email: str,
    document: str,
    cluster_id: int | None,
    status: str = "pending",
) -> None:
    payload = {
        "id": app_id,
        "created_at": "2025-06-01T00:00:00",
        "phone": phone,
        "email": email,
        "document": document,
        "ip": f"10.10.{abs(hash(app_id)) % 200}.{(abs(hash(email)) % 200) + 1}",
        "wallet": f"0x{app_id.replace('_', '')}",
        "payment_wallet": f"T_{app_id.upper()}",
        "address": f"Addr {app_id}",
        "amount": 10000.0,
        "status": status,
    }
    if cluster_id is not None:
        payload["cluster_id"] = cluster_id
    client.add_application(payload)


@pytest.fixture
def isolated_fraud_cluster(client: FraudGraphClient) -> list[str]:
    ids: list[str] = []
    # Fully connected by shared phone, no external overlap.
    for i in range(30):
        app_id = f"iso_{i}"
        _create_app(
            client,
            app_id=app_id,
            phone="+79991110000",
            email=f"iso{i}@fraud.local",
            document=f"9100 {100000 + i}",
            cluster_id=101,
            status="fraud",
        )
        ids.append(app_id)
    return ids


@pytest.fixture
def semi_open_cluster(client: FraudGraphClient) -> list[str]:
    ids: list[str] = []
    # Internal edges: 3 groups x C(10,2) => 135
    # External edges: 6 outside nodes linked to first group (10*6 => 60)
    group_phones = ["+79992220111", "+79992220222", "+79992220333"]

    for g in range(3):
        for i in range(10):
            idx = g * 10 + i
            app_id = f"semi_{idx}"
            _create_app(
                client,
                app_id=app_id,
                phone=group_phones[g],
                email=f"semi{idx}@mix.local",
                document=f"9200 {200000 + idx}",
                cluster_id=202,
                status="pending",
            )
            ids.append(app_id)

    for i in range(6):
        _create_app(
            client,
            app_id=f"semi_ext_{i}",
            phone=group_phones[0],
            email=f"semi.ext{i}@outside.local",
            document=f"9300 {300000 + i}",
            cluster_id=999,
            status="approved",
        )

    return ids


@pytest.fixture
def normal_applications(client: FraudGraphClient) -> list[str]:
    ids: list[str] = []
    for i in range(100):
        # 10 small groups by phone to create random-ish sparse communities
        phone = f"+7999333{(i % 10):04d}"
        app_id = f"norm_{i}"
        _create_app(
            client,
            app_id=app_id,
            phone=phone,
            email=f"norm{i}@normal.local",
            document=f"9400 {400000 + i}",
            cluster_id=303,
            status="approved",
        )
        ids.append(app_id)
    return ids


def test_louvain_finds_isolated_cluster(
    client: FraudGraphClient,
    isolated_fraud_cluster: list[str],
    normal_applications: list[str],
):
    try:
        rows = client.execute_raw(
            """
            MATCH p = (a:Application)-[:LINKED]-(b:Application)
            WITH project(p) AS graph
            CALL community_detection.louvain(graph, {weight_property: 'weight'})
            YIELD node, community_id
            RETURN node.id AS application_id, toInteger(community_id) AS community_id
            """,
            {},
        )
    except Exception as exc:
        pytest.skip(f"MAGE Louvain unavailable: {exc}")

    mapping = {row["application_id"]: row["community_id"] for row in rows}
    cluster_ids = {mapping[app_id] for app_id in isolated_fraud_cluster if app_id in mapping}
    assert len(cluster_ids) == 1, "isolated_fraud_cluster should map to a single Louvain community"


def test_closedness_isolated(client: FraudGraphClient, isolated_fraud_cluster: list[str]):
    closedness = client.calculate_cluster_closedness(101)
    assert closedness > 0.95


def test_closedness_semi_open(client: FraudGraphClient, semi_open_cluster: list[str]):
    closedness = client.calculate_cluster_closedness(202)
    assert 0.60 <= closedness <= 0.80


def test_closedness_normal(client: FraudGraphClient, normal_applications: list[str]):
    closedness = client.calculate_cluster_closedness(303)
    assert closedness < 0.30


def test_get_suspicious_clusters(
    client: FraudGraphClient,
    isolated_fraud_cluster: list[str],
    semi_open_cluster: list[str],
):
    clusters = client.get_suspicious_clusters(closedness_threshold=0.7, min_size=10)
    cluster_ids = {c["cluster_id"] for c in clusters}
    assert 101 in cluster_ids


def test_incremental_assignment(client: FraudGraphClient, isolated_fraud_cluster: list[str]):
    _create_app(
        client,
        app_id="new_iso_link",
        phone="+79991110000",  # shared with isolated cluster
        email="new.iso@fraud.local",
        document="9500 500001",
        cluster_id=None,
        status="pending",
    )

    assigned = client.assign_new_application_to_cluster("new_iso_link")
    assert assigned == 101


def test_cluster_stats_accuracy(client: FraudGraphClient, isolated_fraud_cluster: list[str]):
    stats = client.get_cluster_stats(101)
    assert stats["cluster_id"] == 101
    assert stats["size"] == 30
    assert 0.95 <= stats["closedness"] <= 1.0
    assert stats["avg_internal_weight"] > 0.0
    assert stats["fraud_ratio"] == pytest.approx(1.0)
    assert stats["oldest_app"] is not None
    assert stats["newest_app"] is not None
