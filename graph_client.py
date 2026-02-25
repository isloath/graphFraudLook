"""FraudGraphClient — Memgraph client for the anti-fraud online-lending system.

Requires:
    pip install neo4j

Configure via environment variables:
    MEMGRAPH_HOST  (default: localhost)
    MEMGRAPH_PORT  (default: 7687)
"""

from __future__ import annotations

import os
from typing import Any, Generator

try:
    from neo4j import GraphDatabase, Driver
except ImportError as exc:
    raise ImportError("Install required dependency: pip install neo4j") from exc


class FraudGraphClient:
    """Client for the Application-LINKED fraud graph stored in Memgraph."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
    ) -> None:
        host = host or os.environ.get("MEMGRAPH_HOST", "localhost")
        port = int(port or os.environ.get("MEMGRAPH_PORT", "7687"))
        self._driver: Driver = GraphDatabase.driver(
            f"bolt://{host}:{port}",
            auth=("", ""),
        )

    def close(self) -> None:
        self._driver.close()

    def __enter__(self) -> "FraudGraphClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── core helpers ──────────────────────────────────────────────────────────

    def execute_raw(self, query: str, params: dict | None = None) -> list[dict]:
        """Execute a Cypher query and return all records as plain dicts."""
        with self._driver.session() as session:
            return session.run(query, params or {}).data()

    # ── existing Phase-1/2 methods ────────────────────────────────────────────

    def add_application(self, app_dict: dict) -> str:
        """Insert a new Application node and return its id.

        The trigger in triggers.cypher fires automatically and creates LINKED
        edges to any existing Application that shares one or more attributes.
        """
        query = """
        MERGE (a:Application {id: $id})
        SET a += $props,
            a.created_at = localDateTime($created_at)
        RETURN a.id AS id
        """
        props = {k: v for k, v in app_dict.items() if k not in ("id", "created_at")}
        result = self.execute_raw(
            query,
            {
                "id": app_dict["id"],
                "props": props,
                "created_at": app_dict.get("created_at", "2024-01-01T00:00:00"),
            },
        )
        if not result:
            raise RuntimeError(f"Failed to create application: {app_dict!r}")
        return result[0]["id"]

    def find_by_attribute(
        self,
        attr_type: str,
        value: str,
        limit: int = 100,
    ) -> Generator[dict, None, None]:
        """Yield Application nodes where *attr_type* equals *value*."""
        # attr_type is an internal constant (never user-supplied in production).
        # The index on :Application(attr_type) makes this O(k) where k = results.
        query = (
            f"MATCH (a:Application) WHERE a.{attr_type} = $value "
            f"RETURN a LIMIT $limit"
        )
        for row in self.execute_raw(query, {"value": value, "limit": limit}):
            yield dict(row["a"])

    def get_neighbors(
        self,
        app_id: str,
        depth: int = 1,
        limit: int = 100,
    ) -> Generator[dict, None, None]:
        """Yield Application nodes reachable via LINKED within *depth* hops."""
        query = """
        MATCH (start:Application {id: $app_id})-[:LINKED*1..$depth]-(nb:Application)
        WHERE nb.id <> $app_id
        RETURN DISTINCT nb AS a
        LIMIT $limit
        """
        for row in self.execute_raw(query, {"app_id": app_id, "depth": depth, "limit": limit}):
            yield dict(row["a"])

    def get_application_context(self, app_id: str) -> dict:
        """Return the application node plus its direct LINKED neighbours."""
        query = """
        MATCH (a:Application {id: $app_id})
        OPTIONAL MATCH (a)-[r:LINKED]-(nb:Application)
        RETURN a,
               collect({
                 neighbor_id:  nb.id,
                 weight:        r.weight,
                 shared_attrs:  r.shared_attrs
               }) AS links
        """
        result = self.execute_raw(query, {"app_id": app_id})
        if not result:
            return {}
        row = result[0]
        return {"application": dict(row["a"]), "links": row["links"]}

    # ── cluster metric methods ─────────────────────────────────────────────────

    def calculate_cluster_closedness(self, cluster_id: int) -> float:
        """Return the closedness metric for *cluster_id*.

        closedness = internal_weight / (internal_weight + external_weight)

        • internal_weight — sum of LINKED.weight for edges where **both**
          endpoints belong to *cluster_id*.  Because undirected matching
          traverses each edge in both directions, the raw sum is halved.
        • external_weight — sum of LINKED.weight for edges that leave the
          cluster (counted once per crossing edge, from the cluster side).

        Returns:
            0.0  — all edges exit the cluster (no cohesion), or no edges exist
            1.0  — fully isolated ring (zero external connections)
        """
        query = """
        MATCH (a:Application {cluster_id: $cluster_id})-[r:LINKED]-(b:Application)
        WITH
          sum(CASE WHEN b.cluster_id = $cluster_id
                   THEN r.weight ELSE 0 END) / 2.0  AS internal_weight,
          sum(CASE WHEN b.cluster_id <> $cluster_id OR b.cluster_id IS NULL
                   THEN r.weight ELSE 0 END)         AS external_weight
        RETURN
          CASE
            WHEN (internal_weight + external_weight) > 0
              THEN internal_weight / (internal_weight + external_weight)
            ELSE 0.0
          END AS closedness
        """
        result = self.execute_raw(query, {"cluster_id": cluster_id})
        if not result or result[0]["closedness"] is None:
            return 0.0
        return float(result[0]["closedness"])

    def get_cluster_stats(self, cluster_id: int) -> dict:
        """Return a comprehensive stats dict for *cluster_id* in one query.

        Returns:
            {
              "cluster_id":          int,
              "size":                int,      # node count
              "closedness":          float,    # [0.0, 1.0]
              "avg_internal_weight": float,    # mean weight of intra-cluster edges
              "fraud_ratio":         float,    # fraction of nodes with status='fraud'
              "oldest_app":          datetime, # earliest created_at
              "newest_app":          datetime, # latest  created_at
            }

        Returns {} if *cluster_id* does not exist.

        Query design:
            First MATCH aggregates node-level fields (size, fraud_count,
            oldest/newest timestamps).  The OPTIONAL MATCH that follows
            collects edge-level fields (internal_weight, external_weight).
            OPTIONAL ensures isolated clusters still return a result row
            with zeros for edge stats instead of disappearing entirely.
        """
        query = """
        MATCH (a:Application {cluster_id: $cluster_id})
        WITH
          count(a)                                             AS size,
          sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_count,
          min(a.created_at)                                    AS oldest_app,
          max(a.created_at)                                    AS newest_app

        OPTIONAL MATCH (a:Application {cluster_id: $cluster_id})-[r:LINKED]-(b:Application)
        WITH
          size, fraud_count, oldest_app, newest_app,
          sum(CASE WHEN b.cluster_id = $cluster_id
                   THEN r.weight ELSE 0 END) / 2.0            AS internal_weight,
          sum(CASE WHEN b.cluster_id <> $cluster_id OR b.cluster_id IS NULL
                   THEN r.weight ELSE 0 END)                  AS external_weight,
          toFloat(count(CASE WHEN b.cluster_id = $cluster_id
                             THEN 1 END)) / 2.0               AS internal_edge_count

        RETURN
          size,
          fraud_count,
          oldest_app,
          newest_app,
          internal_weight,
          external_weight,
          internal_edge_count,
          CASE
            WHEN (internal_weight + external_weight) > 0
              THEN internal_weight / (internal_weight + external_weight)
            ELSE 0.0
          END AS closedness,
          CASE
            WHEN internal_edge_count > 0
              THEN internal_weight / internal_edge_count
            ELSE 0.0
          END AS avg_internal_weight
        """
        result = self.execute_raw(query, {"cluster_id": cluster_id})
        if not result or result[0]["size"] is None:
            return {}
        row = result[0]
        size = row["size"]
        return {
            "cluster_id":          cluster_id,
            "size":                size,
            "closedness":          float(row["closedness"] or 0.0),
            "avg_internal_weight": float(row["avg_internal_weight"] or 0.0),
            "fraud_ratio":         row["fraud_count"] / size if size else 0.0,
            "oldest_app":          row["oldest_app"],
            "newest_app":          row["newest_app"],
        }

    def get_all_cluster_stats(self, min_size: int = 5) -> Generator[dict, None, None]:
        """Yield stats dicts for every cluster with at least *min_size* members.

        Designed for dashboard / monitoring loops.  Results are streamed from
        an in-memory list (execute_raw fetches all rows first) so the Bolt
        session is closed before the caller begins iterating.

        Sorted by closedness DESC, then cluster size DESC.
        Clusters with no LINKED edges return 0.0 for all edge-based fields.
        """
        query = """
        MATCH (a:Application)
        WHERE a.cluster_id IS NOT NULL
        WITH
          a.cluster_id                                             AS cid,
          count(a)                                                 AS size,
          sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END)     AS fraud_count,
          min(a.created_at)                                        AS oldest_app,
          max(a.created_at)                                        AS newest_app
        WHERE size >= $min_size

        OPTIONAL MATCH (a:Application {cluster_id: cid})-[r:LINKED]-(b:Application)
        WITH
          cid, size, fraud_count, oldest_app, newest_app,
          sum(CASE WHEN b.cluster_id = cid
                   THEN r.weight ELSE 0 END) / 2.0                AS internal_weight,
          sum(CASE WHEN b.cluster_id <> cid OR b.cluster_id IS NULL
                   THEN r.weight ELSE 0 END)                      AS external_weight,
          toFloat(count(CASE WHEN b.cluster_id = cid
                             THEN 1 END)) / 2.0                   AS internal_edge_count

        WITH
          cid, size, fraud_count, oldest_app, newest_app,
          internal_weight, external_weight, internal_edge_count,
          CASE
            WHEN (internal_weight + external_weight) > 0
              THEN internal_weight / (internal_weight + external_weight)
            ELSE 0.0
          END AS closedness,
          CASE
            WHEN internal_edge_count > 0
              THEN internal_weight / internal_edge_count
            ELSE 0.0
          END AS avg_internal_weight

        RETURN cid, size, fraud_count, oldest_app, newest_app,
               internal_weight, external_weight,
               closedness, avg_internal_weight
        ORDER BY closedness DESC, size DESC
        """
        for row in self.execute_raw(query, {"min_size": min_size}):
            size = row["size"]
            yield {
                "cluster_id":          row["cid"],
                "size":                size,
                "closedness":          float(row["closedness"] or 0.0),
                "avg_internal_weight": float(row["avg_internal_weight"] or 0.0),
                "fraud_ratio":         row["fraud_count"] / size if size else 0.0,
                "oldest_app":          row["oldest_app"],
                "newest_app":          row["newest_app"],
            }

    def get_suspicious_clusters(
        self,
        closedness_threshold: float = 0.7,
        min_size: int = 10,
    ) -> list[dict]:
        """Return suspicious clusters sorted by closedness DESC.

        A cluster is flagged when it satisfies **both**:
            • closedness  > closedness_threshold  (default 0.7)
            • cluster_size >= min_size            (default 10)

        Returns a full list (not a generator) so callers can immediately rank,
        slice, or forward the result to an alerting pipeline.
        Each dict has the same shape as get_cluster_stats().
        """
        query = """
        MATCH (a:Application)
        WHERE a.cluster_id IS NOT NULL
        WITH
          a.cluster_id                                             AS cid,
          count(a)                                                 AS size,
          sum(CASE WHEN a.status = 'fraud' THEN 1 ELSE 0 END)     AS fraud_count,
          min(a.created_at)                                        AS oldest_app,
          max(a.created_at)                                        AS newest_app
        WHERE size >= $min_size

        OPTIONAL MATCH (a:Application {cluster_id: cid})-[r:LINKED]-(b:Application)
        WITH
          cid, size, fraud_count, oldest_app, newest_app,
          sum(CASE WHEN b.cluster_id = cid
                   THEN r.weight ELSE 0 END) / 2.0                AS internal_weight,
          sum(CASE WHEN b.cluster_id <> cid OR b.cluster_id IS NULL
                   THEN r.weight ELSE 0 END)                      AS external_weight,
          toFloat(count(CASE WHEN b.cluster_id = cid
                             THEN 1 END)) / 2.0                   AS internal_edge_count

        WITH
          cid, size, fraud_count, oldest_app, newest_app,
          internal_weight, external_weight, internal_edge_count,
          CASE
            WHEN (internal_weight + external_weight) > 0
              THEN internal_weight / (internal_weight + external_weight)
            ELSE 0.0
          END AS closedness,
          CASE
            WHEN internal_edge_count > 0
              THEN internal_weight / internal_edge_count
            ELSE 0.0
          END AS avg_internal_weight
        WHERE closedness > $closedness_threshold

        RETURN cid, size, fraud_count, oldest_app, newest_app,
               closedness, avg_internal_weight
        ORDER BY closedness DESC, size DESC
        """
        rows = self.execute_raw(
            query,
            {"min_size": min_size, "closedness_threshold": closedness_threshold},
        )
        out = []
        for row in rows:
            size = row["size"]
            out.append({
                "cluster_id":          row["cid"],
                "size":                size,
                "closedness":          float(row["closedness"] or 0.0),
                "avg_internal_weight": float(row["avg_internal_weight"] or 0.0),
                "fraud_ratio":         row["fraud_count"] / size if size else 0.0,
                "oldest_app":          row["oldest_app"],
                "newest_app":          row["newest_app"],
            })
        return out

    def assign_new_application_to_cluster(self, app_id: str) -> int:
        """Incrementally assign *app_id* to a cluster without re-running Louvain.

        Algorithm:
            1. Gather cluster_id from every direct LINKED neighbour.
            2. Weighted majority vote: each neighbour contributes its cluster_id
               weighted by LINKED.weight (heavier connection = stronger signal).
            3. The cluster with the highest total weight is assigned.
            4. Fallback when no neighbour has a cluster_id:
                   new cluster_id = max(existing cluster_id) + 1
               This creates a fresh singleton without disturbing the rest of the
               graph or re-running community detection.

        The trigger in triggers.cypher has already run (BEFORE COMMIT), so LINKED
        edges to existing nodes are available when this method executes.

        Returns:
            The cluster_id written to the Application node.

        Raises:
            ValueError: if *app_id* does not exist in the graph.
        """
        query = """
        MATCH (new:Application {id: $app_id})

        OPTIONAL MATCH (new)-[r:LINKED]-(nb:Application)
        WHERE nb.cluster_id IS NOT NULL

        WITH new,
             nb.cluster_id              AS nb_cid,
             sum(coalesce(r.weight, 1)) AS total_weight
        ORDER BY total_weight DESC

        WITH new, collect(nb_cid)[0] AS majority_cid

        OPTIONAL MATCH (any:Application)
        WHERE any.cluster_id IS NOT NULL
          AND majority_cid IS NULL

        WITH new, majority_cid, max(any.cluster_id) AS max_existing_cid

        SET new.cluster_id = CASE
          WHEN majority_cid     IS NOT NULL THEN majority_cid
          WHEN max_existing_cid IS NOT NULL THEN max_existing_cid + 1
          ELSE 1
        END

        RETURN new.cluster_id AS cluster_id
        """
        result = self.execute_raw(query, {"app_id": app_id})
        if not result:
            raise ValueError(f"Application not found: {app_id!r}")
        return int(result[0]["cluster_id"])
