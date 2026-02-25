"""Memgraph client for fraud graph operations.

Implements a pooled client with parameterized queries, timeouts,
and slow-query logging.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Generator

from neo4j import GraphDatabase, Query


class FraudGraphClient:
    """Client for anti-fraud graph queries in Memgraph."""

    _ALLOWED_ATTRS = {
        "phone",
        "email",
        "document",
        "ip",
        "wallet",
        "payment_wallet",
        "address",
    }

    def __init__(self, host: str, port: int, pool_size: int = 5):
        self._driver = GraphDatabase.driver(
            f"bolt://{host}:{port}",
            max_connection_pool_size=pool_size,
        )
        self._logger = logging.getLogger(self.__class__.__name__)

    def close(self) -> None:
        self._driver.close()

    def _get_connection(self):
        """Get a pooled session/connection."""
        return self._driver.session()

    def _log_slow_query(self, query: str, params: dict[str, Any], duration: float) -> None:
        if duration > 0.100:
            self._logger.warning(
                "Slow query: %.2fms | query=%s | params=%s",
                duration * 1000,
                query,
                params,
            )

    def _execute_with_timeout(
        self,
        query: str,
        params: dict[str, Any] | None,
        timeout: int = 30,
    ) -> list[dict[str, Any]]:
        timeout = min(int(timeout), 30)
        params = params or {}
        start = time.perf_counter()
        with self._get_connection() as session:
            result = session.run(Query(query, timeout=timeout), params)
            rows = [record.data() for record in result]
        elapsed = time.perf_counter() - start
        self._log_slow_query(query, params, elapsed)
        return rows

    def _execute_generator(
        self,
        query: str,
        params: dict[str, Any] | None,
        timeout: int = 30,
    ) -> Generator[dict[str, Any], None, None]:
        timeout = min(int(timeout), 30)
        params = params or {}

        def _gen() -> Generator[dict[str, Any], None, None]:
            start = time.perf_counter()
            with self._get_connection() as session:
                result = session.run(Query(query, timeout=timeout), params)
                for record in result:
                    yield record.data()
            elapsed = time.perf_counter() - start
            self._log_slow_query(query, params, elapsed)

        return _gen()

    def add_application(self, app_dict: dict[str, Any]) -> str:
        app_id = app_dict.get("id")
        if not app_id:
            raise ValueError("app_dict must contain non-empty 'id'")

        query = """
        CREATE (a:Application)
        SET a += $app
        RETURN a.id AS app_id
        """
        rows = self._execute_with_timeout(query, {"app": app_dict})
        return rows[0]["app_id"]

    def find_by_attribute(
        self,
        attr_type: str,
        value: str,
        limit: int = 50,
    ) -> Generator[dict[str, Any], None, None]:
        if attr_type not in self._ALLOWED_ATTRS:
            raise ValueError(f"Unsupported attr_type: {attr_type}")

        query = f"""
        MATCH (a:Application)
        WHERE a.{attr_type} = $value
        RETURN a {{ .* }} AS application
        ORDER BY a.created_at DESC
        LIMIT $limit
        """
        return self._execute_generator(query, {"value": value, "limit": int(limit)})

    def get_neighbors(
        self,
        app_id: str,
        depth: int = 2,
        limit: int = 100,
    ) -> Generator[dict[str, Any], None, None]:
        depth = max(1, min(int(depth), 5))

        query = f"""
        MATCH (root:Application {{id: $app_id}})
        MATCH path = (root)-[rels:LINKED*1..{depth}]-(neighbor:Application)
        WHERE neighbor.id <> $app_id
        WITH neighbor, rels, reduce(w = 0, r IN rels | w + coalesce(r.weight, 0)) AS total_weight
        RETURN DISTINCT
            neighbor {{ .* }} AS neighbor,
            size(rels) AS hops,
            total_weight,
            [r IN rels | r.shared_attrs] AS shared_attrs_chain
        ORDER BY total_weight DESC, hops ASC
        LIMIT $limit
        """
        return self._execute_generator(query, {"app_id": app_id, "limit": int(limit)})

    def get_application_context(self, app_id: str) -> dict[str, Any]:
        query = """
        MATCH (a:Application {id: $app_id})
        OPTIONAL MATCH (a)-[r:LINKED]-(n:Application)
        WITH a,
             collect(DISTINCT n { .* }) AS neighbors,
             count(r) AS degree,
             coalesce(sum(r.weight), 0) AS total_link_weight,
             coalesce(max(r.weight), 0) AS max_link_weight,
             collect(DISTINCT r.shared_attrs) AS shared_attr_sets
        RETURN {
            application: a { .* },
            metrics: {
                degree: degree,
                total_link_weight: total_link_weight,
                max_link_weight: max_link_weight,
                distinct_neighbor_count: size(neighbors)
            },
            neighbors: neighbors,
            shared_attr_sets: shared_attr_sets
        } AS context
        """
        rows = self._execute_with_timeout(query, {"app_id": app_id})
        return rows[0]["context"] if rows else {}

    def get_cluster_members(
        self,
        cluster_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> Generator[dict[str, Any], None, None]:
        query = """
        MATCH (a:Application {cluster_id: $cluster_id})
        RETURN a { .* } AS application
        ORDER BY a.created_at DESC
        SKIP $offset
        LIMIT $limit
        """
        return self._execute_generator(
            query,
            {
                "cluster_id": int(cluster_id),
                "limit": int(limit),
                "offset": int(offset),
            },
        )

    def get_hot_attributes(
        self,
        attr_type: str,
        min_uses: int,
        days: int,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if attr_type not in self._ALLOWED_ATTRS:
            raise ValueError(f"Unsupported attr_type: {attr_type}")

        query = f"""
        MATCH (a:Application)
        WHERE a.created_at >= localDateTime() - duration({{days: $days}})
          AND a.{attr_type} IS NOT NULL
        WITH a.{attr_type} AS attribute_value, count(a) AS uses,
             collect(a.id)[0..5] AS sample_app_ids
        WHERE uses >= $min_uses
        RETURN attribute_value, uses, sample_app_ids
        ORDER BY uses DESC
        LIMIT $limit
        """
        return self._execute_with_timeout(
            query,
            {
                "days": int(days),
                "min_uses": int(min_uses),
                "limit": int(limit),
            },
        )


    def calculate_cluster_closedness(self, cluster_id: int) -> float:
        """Calculate cluster closedness in range [0.0, 1.0]."""
        query = """
        MATCH (a:Application {cluster_id: $cluster_id})
        OPTIONAL MATCH (a)-[r:LINKED]-(b:Application)
        WITH
            sum(CASE
                WHEN b.cluster_id = $cluster_id AND id(a) < id(b)
                THEN coalesce(r.weight, 0)
                ELSE 0
            END) AS internal_weight,
            sum(CASE
                WHEN b.cluster_id <> $cluster_id
                THEN coalesce(r.weight, 0)
                ELSE 0
            END) AS external_weight
        RETURN CASE
            WHEN internal_weight + external_weight = 0 THEN 0.0
            ELSE toFloat(internal_weight) / toFloat(internal_weight + external_weight)
        END AS closedness
        """
        rows = self._execute_with_timeout(query, {"cluster_id": int(cluster_id)})
        return float(rows[0]["closedness"]) if rows else 0.0

    def get_cluster_stats(self, cluster_id: int) -> dict[str, Any]:
        """Return aggregated statistics for a single cluster."""
        query = """
        MATCH (a:Application {cluster_id: $cluster_id})
        WITH collect(a) AS nodes,
             count(a) AS size,
             min(a.created_at) AS oldest_app,
             max(a.created_at) AS newest_app,
             sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_count
        UNWIND nodes AS n
        OPTIONAL MATCH (n)-[r:LINKED]-(m:Application)
        WITH size, oldest_app, newest_app, fraud_count,
             sum(CASE
                 WHEN m.cluster_id = $cluster_id AND id(n) < id(m)
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS internal_weight,
             sum(CASE
                 WHEN m.cluster_id = $cluster_id AND id(n) < id(m)
                 THEN 1
                 ELSE 0
             END) AS internal_edges,
             sum(CASE
                 WHEN m.cluster_id <> $cluster_id
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS external_weight
        RETURN {
            cluster_id: $cluster_id,
            size: size,
            closedness: CASE
                WHEN internal_weight + external_weight = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_weight + external_weight)
            END,
            avg_internal_weight: CASE
                WHEN internal_edges = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_edges)
            END,
            fraud_ratio: CASE
                WHEN size = 0 THEN 0.0
                ELSE toFloat(fraud_count) / toFloat(size)
            END,
            oldest_app: oldest_app,
            newest_app: newest_app
        } AS cluster_stats
        """
        rows = self._execute_with_timeout(query, {"cluster_id": int(cluster_id)})
        return rows[0]["cluster_stats"] if rows else {}

    def get_all_cluster_stats(self, min_size: int = 5) -> Generator[dict[str, Any], None, None]:
        """Stream stats for all clusters with size >= min_size."""
        query = """
        MATCH (a:Application)
        WHERE a.cluster_id IS NOT NULL
        WITH a.cluster_id AS cluster_id,
             collect(a) AS nodes,
             count(a) AS size,
             min(a.created_at) AS oldest_app,
             max(a.created_at) AS newest_app,
             sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_count
        WHERE size >= $min_size
        UNWIND nodes AS n
        OPTIONAL MATCH (n)-[r:LINKED]-(m:Application)
        WITH cluster_id, size, oldest_app, newest_app, fraud_count,
             sum(CASE
                 WHEN m.cluster_id = cluster_id AND id(n) < id(m)
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS internal_weight,
             sum(CASE
                 WHEN m.cluster_id = cluster_id AND id(n) < id(m)
                 THEN 1
                 ELSE 0
             END) AS internal_edges,
             sum(CASE
                 WHEN m.cluster_id <> cluster_id
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS external_weight
        RETURN {
            cluster_id: cluster_id,
            size: size,
            closedness: CASE
                WHEN internal_weight + external_weight = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_weight + external_weight)
            END,
            avg_internal_weight: CASE
                WHEN internal_edges = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_edges)
            END,
            fraud_ratio: CASE
                WHEN size = 0 THEN 0.0
                ELSE toFloat(fraud_count) / toFloat(size)
            END,
            oldest_app: oldest_app,
            newest_app: newest_app
        } AS cluster_stats
        ORDER BY cluster_stats.closedness DESC, cluster_stats.size DESC
        """
        rows = self._execute_generator(query, {"min_size": int(min_size)})

        def _gen() -> Generator[dict[str, Any], None, None]:
            for row in rows:
                yield row["cluster_stats"]

        return _gen()

    def get_suspicious_clusters(
        self,
        closedness_threshold: float = 0.7,
        min_size: int = 10,
    ) -> list[dict[str, Any]]:
        """Return suspicious clusters sorted by closedness DESC."""
        query = """
        MATCH (a:Application)
        WHERE a.cluster_id IS NOT NULL
        WITH a.cluster_id AS cluster_id,
             collect(a) AS nodes,
             count(a) AS size,
             min(a.created_at) AS oldest_app,
             max(a.created_at) AS newest_app,
             sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_count
        WHERE size >= $min_size
        UNWIND nodes AS n
        OPTIONAL MATCH (n)-[r:LINKED]-(m:Application)
        WITH cluster_id, size, oldest_app, newest_app, fraud_count,
             sum(CASE
                 WHEN m.cluster_id = cluster_id AND id(n) < id(m)
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS internal_weight,
             sum(CASE
                 WHEN m.cluster_id = cluster_id AND id(n) < id(m)
                 THEN 1
                 ELSE 0
             END) AS internal_edges,
             sum(CASE
                 WHEN m.cluster_id <> cluster_id
                 THEN coalesce(r.weight, 0)
                 ELSE 0
             END) AS external_weight
        WITH {
            cluster_id: cluster_id,
            size: size,
            closedness: CASE
                WHEN internal_weight + external_weight = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_weight + external_weight)
            END,
            avg_internal_weight: CASE
                WHEN internal_edges = 0 THEN 0.0
                ELSE toFloat(internal_weight) / toFloat(internal_edges)
            END,
            fraud_ratio: CASE
                WHEN size = 0 THEN 0.0
                ELSE toFloat(fraud_count) / toFloat(size)
            END,
            oldest_app: oldest_app,
            newest_app: newest_app
        } AS stats
        WHERE stats.closedness >= $closedness_threshold
        RETURN stats
        ORDER BY stats.closedness DESC, stats.size DESC
        """
        rows = self._execute_with_timeout(
            query,
            {
                "closedness_threshold": float(closedness_threshold),
                "min_size": int(min_size),
            },
        )
        return [row["stats"] for row in rows]

    def assign_new_application_to_cluster(self, app_id: str) -> int:
        """Assign new application to majority neighbor cluster or create a new one."""
        query = """
        MATCH (new_app:Application {id: $app_id})
        OPTIONAL MATCH (new_app)-[:LINKED]-(nbr:Application)
        WHERE nbr.cluster_id IS NOT NULL
        WITH new_app, nbr.cluster_id AS cid, count(*) AS votes
        ORDER BY votes DESC, cid ASC
        WITH new_app, collect({cid: cid, votes: votes}) AS ranked
        CALL {
            WITH new_app, ranked
            WITH new_app, ranked[0] AS top_choice
            WHERE top_choice.cid IS NOT NULL
            SET new_app.cluster_id = top_choice.cid
            RETURN toInteger(new_app.cluster_id) AS assigned_cluster_id

            UNION

            WITH new_app, ranked
            WHERE size([x IN ranked WHERE x.cid IS NOT NULL]) = 0
            MATCH (a:Application)
            WITH new_app, coalesce(max(a.cluster_id), 0) + 1 AS new_cluster_id
            SET new_app.cluster_id = new_cluster_id
            RETURN toInteger(new_cluster_id) AS assigned_cluster_id
        }
        RETURN assigned_cluster_id
        """
        rows = self._execute_with_timeout(query, {"app_id": app_id})
        if not rows:
            raise ValueError(f"Application not found: {app_id}")
        return int(rows[0]["assigned_cluster_id"])

    def execute_raw(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return self._execute_with_timeout(query, params)

    def __enter__(self) -> "FraudGraphClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
