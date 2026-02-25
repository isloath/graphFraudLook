#!/usr/bin/env python3
"""FraudGraphClient — Memgraph client for the anti-fraud lending system.

Provides connection pooling, parameterized queries, generator-based
result streaming, query timeouts (30 s), and slow-query logging (>100 ms).

Usage::

    from graph_client import FraudGraphClient

    with FraudGraphClient("localhost", 7687) as client:
        app_id = client.add_application({
            "id": "app_001", "phone": "+79001234567", ...
        })
        for row in client.find_by_attribute("phone", "+79001234567"):
            print(row)

Depends on: ``pip install neo4j``  (Bolt driver compatible with Memgraph)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError
except ImportError as exc:
    raise ImportError(
        "neo4j driver is required: pip install neo4j"
    ) from exc

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

_SLOW_QUERY_MS = 100          # log queries slower than this (milliseconds)
_DEFAULT_TIMEOUT = 30         # server-side transaction timeout (seconds)

_ATTR_TYPES = frozenset({
    "phone", "email", "document", "ip",
    "wallet", "payment_wallet", "address",
})

# ── pre-built query strings ─────────────────────────────────────────────────
#
# Property names are baked into the query at import time (NOT concatenated
# from user input) so every value is always passed as a $parameter.
# This is equivalent to the separate 5a-5g variants in queries.cypher.

# -- find_by_attribute (one per attr type) --
_FIND_BY_ATTR: dict[str, str] = {}
for _attr in _ATTR_TYPES:
    _FIND_BY_ATTR[_attr] = (
        "MATCH (a:Application) "
        f"WHERE a.{_attr} = $value "
        "RETURN a.id          AS id,"
        "       a.created_at  AS created_at,"
        "       a.phone       AS phone,"
        "       a.email       AS email,"
        "       a.document    AS document,"
        "       a.ip          AS ip,"
        "       a.wallet      AS wallet,"
        "       a.payment_wallet AS payment_wallet,"
        "       a.address     AS address,"
        "       a.amount      AS amount,"
        "       a.status      AS status,"
        "       a.cluster_id  AS cluster_id,"
        "       a.fraud_score AS fraud_score "
        "ORDER BY a.created_at DESC "
        "LIMIT $limit"
    )

# -- get_hot_attributes (one per attr type) --
_HOT_ATTR: dict[str, str] = {}
for _attr in _ATTR_TYPES:
    _HOT_ATTR[_attr] = (
        "MATCH (a:Application) "
        "WHERE a.created_at >= localDateTime($since_date) "
        f"  AND a.{_attr} IS NOT NULL "
        f"WITH a.{_attr} AS attr_value, count(a) AS use_count "
        "WHERE use_count >= $min_uses "
        f"RETURN '{_attr}' AS attr_type,"
        "       attr_value,"
        "       use_count "
        "ORDER BY use_count DESC "
        "LIMIT $limit"
    )

# -- add_application --
_ADD_APP = (
    "CREATE (a:Application) "
    "SET a += $props, "
    "    a.created_at = localDateTime($created_at) "
    "RETURN a.id AS id"
)

# -- get_neighbors (one variant per depth) --
_NEIGHBORS_D1 = (
    "MATCH (src:Application {id: $app_id})-[r:LINKED]-(neighbor:Application) "
    "RETURN neighbor.id          AS id,"
    "       neighbor.status      AS status,"
    "       neighbor.fraud_score AS fraud_score,"
    "       neighbor.cluster_id  AS cluster_id,"
    "       r.weight             AS link_weight,"
    "       r.shared_attrs       AS shared_attrs,"
    "       1                    AS hops "
    "ORDER BY r.weight DESC "
    "LIMIT $limit"
)

_NEIGHBORS_D2 = (
    "MATCH path = (src:Application {id: $app_id})"
    "-[:LINKED*1..2]-(neighbor:Application) "
    "WHERE neighbor.id <> $app_id "
    "WITH DISTINCT neighbor, "
    "     min(size(relationships(path))) AS hops "
    "RETURN neighbor.id          AS id,"
    "       neighbor.status      AS status,"
    "       neighbor.fraud_score AS fraud_score,"
    "       neighbor.cluster_id  AS cluster_id,"
    "       hops "
    "ORDER BY hops, neighbor.fraud_score DESC "
    "LIMIT $limit"
)

_NEIGHBORS_D3 = (
    "MATCH path = (src:Application {id: $app_id})"
    "-[:LINKED*1..3]-(neighbor:Application) "
    "WHERE neighbor.id <> $app_id "
    "WITH DISTINCT neighbor, "
    "     min(size(relationships(path))) AS hops "
    "RETURN neighbor.id          AS id,"
    "       neighbor.status      AS status,"
    "       neighbor.fraud_score AS fraud_score,"
    "       neighbor.cluster_id  AS cluster_id,"
    "       hops "
    "ORDER BY hops, neighbor.fraud_score DESC "
    "LIMIT $limit"
)

_NEIGHBORS = {1: _NEIGHBORS_D1, 2: _NEIGHBORS_D2, 3: _NEIGHBORS_D3}

# -- get_shared_applications --
_SHARED_APPS = (
    "MATCH (src:Application {id: $app_id})-[r:LINKED]-(app:Application) "
    "WHERE r.weight >= $min_shared "
    "RETURN app.id          AS id,"
    "       app.phone       AS phone,"
    "       app.email       AS email,"
    "       app.document    AS document,"
    "       app.status      AS status,"
    "       app.fraud_score AS fraud_score,"
    "       r.weight        AS weight,"
    "       r.shared_attrs  AS shared_attrs "
    "ORDER BY r.weight DESC, app.fraud_score DESC "
    "LIMIT $limit"
)

# -- get_cluster_members --
_CLUSTER_MEMBERS = (
    "MATCH (a:Application) "
    "WHERE a.cluster_id = $cluster_id "
    "RETURN a.id          AS id,"
    "       a.created_at  AS created_at,"
    "       a.phone       AS phone,"
    "       a.email       AS email,"
    "       a.document    AS document,"
    "       a.amount      AS amount,"
    "       a.status      AS status,"
    "       a.fraud_score AS fraud_score "
    "ORDER BY a.created_at DESC "
    "SKIP $offset "
    "LIMIT $limit"
)

# -- get_application_with_context --
_APP_CONTEXT = (
    "MATCH (app:Application {id: $app_id}) "
    "OPTIONAL MATCH (app)-[r:LINKED]-(neighbor:Application) "
    "WITH app,"
    "     count(neighbor) AS neighbor_count,"
    "     sum(CASE WHEN neighbor.status = 'fraud' THEN 1 ELSE 0 END)"
    "       AS fraud_neighbor_count,"
    "     max(r.weight) AS max_link_weight,"
    "     avg(r.weight) AS avg_link_weight "
    "RETURN app.id              AS id,"
    "       app.created_at      AS created_at,"
    "       app.phone           AS phone,"
    "       app.email           AS email,"
    "       app.document        AS document,"
    "       app.ip              AS ip,"
    "       app.wallet          AS wallet,"
    "       app.payment_wallet  AS payment_wallet,"
    "       app.address         AS address,"
    "       app.amount          AS amount,"
    "       app.status          AS status,"
    "       app.cluster_id      AS cluster_id,"
    "       app.fraud_score     AS fraud_score,"
    "       neighbor_count,"
    "       fraud_neighbor_count,"
    "       max_link_weight,"
    "       avg_link_weight"
)

# -- get_recent_applications_velocity --
_VELOCITY = (
    "MATCH (a:Application) "
    "WHERE a.created_at >= localDateTime($since) "
    "OPTIONAL MATCH (a)-[r:LINKED]-(neighbor:Application) "
    "WITH a,"
    "     count(r) AS link_count,"
    "     sum(CASE WHEN neighbor.status = 'fraud' THEN 1 ELSE 0 END)"
    "       AS fraud_link_count "
    "RETURN a.id          AS id,"
    "       a.created_at  AS created_at,"
    "       a.phone       AS phone,"
    "       a.email       AS email,"
    "       a.amount      AS amount,"
    "       a.status      AS status,"
    "       a.fraud_score AS fraud_score,"
    "       link_count,"
    "       fraud_link_count "
    "ORDER BY link_count DESC "
    "LIMIT $limit"
)

# cleanup module-level loop variable
del _attr


# ── client ───────────────────────────────────────────────────────────────────

class FraudGraphClient:
    """Memgraph client for the anti-fraud online lending system.

    Features:
      - Connection pooling (neo4j driver manages the pool internally)
      - Every query is parameterized — no string concatenation
      - Generator-based result streaming for large result sets
      - Per-transaction timeout (default 30 s, server-enforced)
      - Automatic logging of queries slower than 100 ms
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7687,
        pool_size: int = 5,
    ) -> None:
        uri = f"bolt://{host}:{port}"
        self._driver = GraphDatabase.driver(
            uri,
            max_connection_pool_size=pool_size,
            connection_acquisition_timeout=_DEFAULT_TIMEOUT,
        )
        logger.info("FraudGraphClient connected to %s (pool_size=%d)", uri, pool_size)

    # -- context manager ---------------------------------------------------

    def __enter__(self) -> FraudGraphClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Shut down the driver and release all pooled connections."""
        self._driver.close()
        logger.info("FraudGraphClient connection closed")

    # -- internal helpers --------------------------------------------------

    def _get_connection(self):
        """Acquire a session (connection) from the pool."""
        return self._driver.session()

    def _log_slow_query(
        self,
        query: str,
        params: dict[str, Any],
        duration_ms: float,
    ) -> None:
        # Truncate the query text for readable log lines.
        short = query[:120].replace("\n", " ")
        if len(query) > 120:
            short += "…"
        logger.warning(
            "SLOW QUERY (%.1f ms): %s | params=%s",
            duration_ms,
            short,
            params,
        )

    def _execute_with_timeout(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> list[dict[str, Any]]:
        """Run *query* eagerly and return all rows as a list of dicts.

        Uses an explicit transaction with a server-side *timeout*.
        Commits after reading; rolls back on error.
        """
        params = params or {}
        start = time.monotonic()
        session = self._driver.session()
        try:
            with session.begin_transaction(timeout=timeout) as tx:
                result = tx.run(query, params)
                records = [record.data() for record in result]
                tx.commit()
                return records
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            if elapsed_ms > _SLOW_QUERY_MS:
                self._log_slow_query(query, params, elapsed_ms)
            session.close()

    def _generate_results(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> Generator[dict[str, Any], None, None]:
        """Run *query* and lazily yield rows as dicts.

        The underlying session stays open while the caller iterates.
        Cleanup (transaction close + slow-query log) runs when the
        generator is fully consumed **or** garbage-collected.
        """
        params = params or {}
        start = time.monotonic()
        session = self._driver.session()
        try:
            tx = session.begin_transaction(timeout=timeout)
            try:
                result = tx.run(query, params)
                for record in result:
                    yield record.data()
            finally:
                tx.close()
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            if elapsed_ms > _SLOW_QUERY_MS:
                self._log_slow_query(query, params, elapsed_ms)
            session.close()

    # -- public API --------------------------------------------------------

    def add_application(self, app_dict: dict[str, Any]) -> str:
        """Create an Application node and return its *id*.

        The Memgraph trigger (``link_new_applications``) will
        automatically create LINKED edges to existing Applications
        that share attributes.
        """
        props = dict(app_dict)  # shallow copy — don't mutate caller's dict

        if "created_at" not in props:
            props["created_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        if "id" not in props:
            raise ValueError("app_dict must contain an 'id' key")

        params: dict[str, Any] = {
            "props": props,
            "created_at": props["created_at"],
        }

        rows = self._execute_with_timeout(_ADD_APP, params)
        return rows[0]["id"]

    def find_by_attribute(
        self,
        attr_type: str,
        value: str,
        limit: int = 50,
    ) -> Generator[dict[str, Any], None, None]:
        """Find Applications by an indexed attribute value.

        *attr_type* must be one of: phone, email, document, ip,
        wallet, payment_wallet, address.
        """
        if attr_type not in _ATTR_TYPES:
            raise ValueError(
                f"Unknown attr_type {attr_type!r}. "
                f"Must be one of: {', '.join(sorted(_ATTR_TYPES))}"
            )
        return self._generate_results(
            _FIND_BY_ATTR[attr_type],
            {"value": value, "limit": limit},
        )

    def get_neighbors(
        self,
        app_id: str,
        depth: int = 2,
        limit: int = 100,
    ) -> Generator[dict[str, Any], None, None]:
        """Return neighbors reachable through LINKED edges up to *depth* hops.

        *depth* must be 1, 2, or 3.
        """
        if depth not in _NEIGHBORS:
            raise ValueError(f"depth must be 1, 2, or 3 (got {depth})")
        return self._generate_results(
            _NEIGHBORS[depth],
            {"app_id": app_id, "limit": limit},
        )

    def get_shared_applications(
        self,
        app_id: str,
        min_shared: int = 2,
        limit: int = 100,
    ) -> Generator[dict[str, Any], None, None]:
        """Applications linked to *app_id* with >= *min_shared* common attrs."""
        return self._generate_results(
            _SHARED_APPS,
            {"app_id": app_id, "min_shared": min_shared, "limit": limit},
        )

    def get_application_context(self, app_id: str) -> dict[str, Any] | None:
        """Full context for one application — single DB round-trip.

        Returns the application properties together with aggregated
        neighbor metrics (neighbor_count, fraud_neighbor_count,
        max_link_weight, avg_link_weight).

        Returns ``None`` if *app_id* does not exist.
        """
        rows = self._execute_with_timeout(_APP_CONTEXT, {"app_id": app_id})
        if not rows:
            return None
        return rows[0]

    def get_cluster_members(
        self,
        cluster_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> Generator[dict[str, Any], None, None]:
        """Paginated retrieval of all Applications in a fraud cluster."""
        return self._generate_results(
            _CLUSTER_MEMBERS,
            {"cluster_id": cluster_id, "limit": limit, "offset": offset},
        )

    def get_hot_attributes(
        self,
        attr_type: str,
        min_uses: int,
        days: int,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Attributes used by more than *min_uses* applications in last *days* days.

        Returns a list (not generator) because the result set is typically
        small and fully consumed by the caller for alerting / dashboards.
        """
        if attr_type not in _ATTR_TYPES:
            raise ValueError(
                f"Unknown attr_type {attr_type!r}. "
                f"Must be one of: {', '.join(sorted(_ATTR_TYPES))}"
            )
        since = (datetime.now() - timedelta(days=days)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        return self._execute_with_timeout(
            _HOT_ATTR[attr_type],
            {"since_date": since, "min_uses": min_uses, "limit": limit},
        )

    def get_velocity(
        self,
        days: int = 1,
        limit: int = 100,
    ) -> Generator[dict[str, Any], None, None]:
        """Recent applications ordered by number of new LINKED edges.

        Useful for velocity monitoring — identifies applications that
        instantly formed many connections (potential fraud burst).
        """
        since = (datetime.now() - timedelta(days=days)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        return self._generate_results(
            _VELOCITY,
            {"since": since, "limit": limit},
        )

    def execute_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Run an arbitrary Cypher query and return results as dicts.

        Intended for ad-hoc / custom queries not covered by the
        dedicated methods above.
        """
        return self._execute_with_timeout(query, params)
