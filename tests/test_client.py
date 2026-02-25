"""Integration tests for FraudGraphClient.

Requires running Memgraph on bolt://localhost:7687.
"""

from __future__ import annotations

import os
import time
import types
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


@pytest.fixture
def sample_applications(client: FraudGraphClient) -> list[dict]:
    apps: list[dict] = []

    # 10 linked applications (shared phone)
    shared_phone = "+79005550000"
    for i in range(10):
        app = {
            "id": f"sample_linked_{i}",
            "created_at": f"2025-01-01T00:00:{i:02d}",
            "phone": shared_phone,
            "email": f"linked{i}@test.local",
            "document": f"5000 {100000 + i}",
            "ip": f"10.0.0.{i + 1}",
            "wallet": f"0xlinked{i:02d}",
            "payment_wallet": f"T_LINKED_{i:02d}",
            "address": f"Linked st, {i}",
            "amount": 10000.0 + i,
            "status": "pending",
            "cluster_id": 1,
            "fraud_score": 0.7,
        }
        client.add_application(app)
        apps.append(app)

    # 90 mostly random applications
    for i in range(90):
        app = {
            "id": f"sample_random_{i}",
            "created_at": f"2025-01-02T00:00:{i % 60:02d}",
            "phone": f"+7900666{i:04d}",
            "email": f"random{i}@test.local",
            "document": f"6000 {200000 + i}",
            "ip": f"192.168.10.{(i % 250) + 1}",
            "wallet": f"0xrandom{i:02d}",
            "payment_wallet": f"T_RANDOM_{i:02d}",
            "address": f"Random st, {i}",
            "amount": 5000.0 + i,
            "status": "approved",
            "cluster_id": 0,
            "fraud_score": 0.1,
        }
        client.add_application(app)
        apps.append(app)

    return apps


@pytest.fixture
def fraud_cluster(client: FraudGraphClient) -> list[dict]:
    apps: list[dict] = []
    for i in range(20):
        app = {
            "id": f"fraud_{i}",
            "created_at": f"2025-01-03T00:01:{i:02d}",
            "phone": "+79990001111",
            "email": f"fraud{i}@ring.local",
            "document": "7777 123456",
            "ip": f"172.16.1.{(i % 250) + 1}",
            "wallet": f"0xfraud{i:02d}",
            "payment_wallet": "T_FRAUD_COMMON",
            "address": f"Fraud ave, {i}",
            "amount": 15000.0 + i,
            "status": "pending",
            "cluster_id": 77,
            "fraud_score": 0.95,
        }
        client.add_application(app)
        apps.append(app)
    return apps


def test_add_application(client: FraudGraphClient):
    app = {
        "id": "add_001",
        "created_at": "2025-01-01T12:00:00",
        "phone": "+79001230000",
        "email": "add@test.local",
        "document": "1111 222222",
        "amount": 12345.67,
        "status": "pending",
    }

    app_id = client.add_application(app)
    assert app_id == "add_001"

    rows = client.execute_raw(
        "MATCH (a:Application {id: $id}) RETURN a.id AS id, a.phone AS phone",
        {"id": "add_001"},
    )
    assert len(rows) == 1
    assert rows[0]["id"] == "add_001"
    assert rows[0]["phone"] == "+79001230000"


def test_linked_created_on_insert(client: FraudGraphClient):
    client.add_application(
        {
            "id": "link_a",
            "created_at": "2025-01-01T10:00:00",
            "phone": "+79009990000",
            "email": "a@test.local",
            "document": "1111 111111",
            "amount": 10000.0,
            "status": "pending",
        }
    )
    client.add_application(
        {
            "id": "link_b",
            "created_at": "2025-01-01T10:00:10",
            "phone": "+79009990000",
            "email": "b@test.local",
            "document": "2222 222222",
            "amount": 12000.0,
            "status": "pending",
        }
    )

    rows = client.execute_raw(
        """
        MATCH (:Application {id: $b})-[r:LINKED]-(:Application {id: $a})
        RETURN count(r) AS cnt, max(r.weight) AS w
        """,
        {"a": "link_a", "b": "link_b"},
    )
    assert rows[0]["cnt"] == 1
    assert rows[0]["w"] >= 1


def test_find_by_phone(client: FraudGraphClient, sample_applications: list[dict]):
    result = list(client.find_by_attribute("phone", "+79005550000", limit=50))
    assert len(result) == 10
    assert all(r["application"]["phone"] == "+79005550000" for r in result)


def test_get_neighbors_depth1(client: FraudGraphClient, fraud_cluster: list[dict]):
    rows = list(client.get_neighbors("fraud_0", depth=1, limit=100))
    assert rows, "Depth=1 should return direct LINKED neighbors"
    ids = {row["neighbor"]["id"] for row in rows}
    assert "fraud_1" in ids


def test_get_neighbors_depth2(client: FraudGraphClient):
    # create explicit chain A-B-C using shared phone
    client.add_application(
        {
            "id": "n2_a",
            "created_at": "2025-02-01T00:00:00",
            "phone": "+79001230001",
            "email": "a@n2.local",
            "document": "1234 000001",
            "amount": 10000.0,
            "status": "pending",
        }
    )
    client.add_application(
        {
            "id": "n2_b",
            "created_at": "2025-02-01T00:00:01",
            "phone": "+79001230001",
            "email": "b@n2.local",
            "document": "1234 000002",
            "amount": 10000.0,
            "status": "pending",
        }
    )
    client.add_application(
        {
            "id": "n2_c",
            "created_at": "2025-02-01T00:00:02",
            "phone": "+79001230002",
            "email": "b@n2.local",  # links b-c by email
            "document": "1234 000003",
            "amount": 10000.0,
            "status": "pending",
        }
    )

    rows = list(client.get_neighbors("n2_a", depth=2, limit=100))
    ids = {row["neighbor"]["id"] for row in rows}
    assert "n2_b" in ids
    assert "n2_c" in ids, "Depth=2 should include friends-of-friends"


def test_get_neighbors_limit(client: FraudGraphClient, fraud_cluster: list[dict]):
    rows = list(client.get_neighbors("fraud_0", depth=1, limit=5))
    assert len(rows) <= 5


def test_get_application_context(client: FraudGraphClient, fraud_cluster: list[dict]):
    context = client.get_application_context("fraud_0")
    assert context
    assert context["application"]["id"] == "fraud_0"
    assert "metrics" in context
    assert context["metrics"]["degree"] >= 1
    assert isinstance(context["neighbors"], list)


def test_generator_memory(client: FraudGraphClient):
    # create star graph with 1000 linked nodes
    client.add_application(
        {
            "id": "hub",
            "created_at": "2025-03-01T00:00:00",
            "phone": "+79008889999",
            "email": "hub@test.local",
            "document": "8080 100000",
            "amount": 10000.0,
            "status": "pending",
        }
    )

    for i in range(1000):
        client.add_application(
            {
                "id": f"leaf_{i}",
                "created_at": f"2025-03-01T00:00:{i % 60:02d}",
                "phone": "+79008889999",
                "email": f"leaf{i}@test.local",
                "document": f"8080 {100001 + i}",
                "amount": 10000.0,
                "status": "pending",
            }
        )

    gen = client.get_neighbors("hub", depth=1, limit=1000)
    assert isinstance(gen, types.GeneratorType)

    # partial consumption should work without materializing list() eagerly
    first_ten = []
    for idx, row in enumerate(gen):
        first_ten.append(row)
        if idx == 9:
            break

    assert len(first_ten) == 10


@pytest.mark.slow
def test_find_performance(client: FraudGraphClient, sample_applications: list[dict]):
    start = time.perf_counter()
    for _ in range(1000):
        rows = list(client.find_by_attribute("phone", "+79005550000", limit=20))
        assert rows
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0, f"1000 find_by_attribute calls took {elapsed:.3f}s"


@pytest.mark.slow
def test_neighbors_performance(client: FraudGraphClient):
    # 10k nodes split by 100 shared phones for connectivity
    for i in range(10_000):
        client.add_application(
            {
                "id": f"perf_{i}",
                "created_at": f"2025-04-01T00:00:{i % 60:02d}",
                "phone": f"+7999000{(i % 100):04d}",
                "email": f"perf{i}@test.local",
                "document": f"9090 {200000 + i}",
                "amount": 20000.0,
                "status": "pending",
            }
        )

    start = time.perf_counter()
    rows = list(client.get_neighbors("perf_0", depth=2, limit=200))
    elapsed = time.perf_counter() - start

    assert rows
    assert elapsed < 0.100, f"get_neighbors(depth=2) took {elapsed * 1000:.2f}ms"
