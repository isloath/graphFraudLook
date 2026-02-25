// Memgraph MAGE clustering queries for fraud-graph
// ------------------------------------------------
// Assumptions:
// - Nodes: (:Application)
// - Existing edges: [:LINKED {weight, shared_attrs, created_at}]
// - MAGE Louvain is executed on a projected graph

// 1) Build Application<->Application projection from existing LINKED edges.
// Returns in-memory projected graph object.
MATCH p = (a:Application)-[r:LINKED]-(b:Application)
WITH project(p) AS graph
RETURN graph;


// 2) Run Louvain community detection on projection.
// weight_property="weight" to use LINKED.weight as edge weight.
MATCH p = (a:Application)-[r:LINKED]-(b:Application)
WITH project(p) AS graph
CALL community_detection.louvain(graph, {weight_property: "weight"})
YIELD node, community_id
RETURN id(node) AS node_id, node.id AS application_id, toInteger(community_id) AS community_id
ORDER BY community_id, application_id;


// 3) Save Louvain result into Application.cluster_id in batches of 1000.
// Single query, batched SET for better throughput on large graphs.
MATCH p = (a:Application)-[r:LINKED]-(b:Application)
WITH project(p) AS graph
CALL community_detection.louvain(graph, {weight_property: "weight"})
YIELD node, community_id
WITH collect({node: node, cluster_id: toInteger(community_id)}) AS rows
UNWIND range(0, size(rows) - 1, 1000) AS batch_start
UNWIND rows[batch_start..(batch_start + 1000)] AS row
SET row.node.cluster_id = row.cluster_id
RETURN count(*) AS updated_nodes;


// 4.1) Cluster statistics: total number of non-empty clusters.
MATCH (a:Application)
WHERE a.cluster_id IS NOT NULL
RETURN count(DISTINCT a.cluster_id) AS cluster_count;


// 4.2) Cluster statistics: size of each cluster.
MATCH (a:Application)
WHERE a.cluster_id IS NOT NULL
RETURN a.cluster_id AS cluster_id, count(*) AS cluster_size
ORDER BY cluster_size DESC, cluster_id;


// 4.3) Cluster statistics: distribution by cluster size.
MATCH (a:Application)
WHERE a.cluster_id IS NOT NULL
WITH a.cluster_id AS cluster_id, count(*) AS cluster_size
RETURN cluster_size, count(*) AS clusters_with_this_size
ORDER BY cluster_size DESC;


// 4.4) Optional bucketed distribution (1, 2-5, 6-10, 11-50, 51+).
MATCH (a:Application)
WHERE a.cluster_id IS NOT NULL
WITH a.cluster_id AS cluster_id, count(*) AS cluster_size
WITH CASE
    WHEN cluster_size = 1 THEN "1"
    WHEN cluster_size <= 5 THEN "2-5"
    WHEN cluster_size <= 10 THEN "6-10"
    WHEN cluster_size <= 50 THEN "11-50"
    ELSE "51+"
END AS size_bucket
RETURN size_bucket, count(*) AS cluster_count
ORDER BY size_bucket;


// 5) Incremental mode: assign_to_nearest_cluster for a new application.
// Input parameter: $app_id (new application id)
// Rule:
// - assign majority cluster among neighbors
// - if no neighbors with cluster_id -> create new cluster = max(cluster_id)+1
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
    RETURN new_app.cluster_id AS assigned_cluster_id, "existing" AS assignment_mode

    UNION

    WITH new_app, ranked
    WHERE size([x IN ranked WHERE x.cid IS NOT NULL]) = 0
    MATCH (a:Application)
    WITH new_app, coalesce(max(a.cluster_id), 0) + 1 AS new_cluster_id
    SET new_app.cluster_id = new_cluster_id
    RETURN new_cluster_id AS assigned_cluster_id, "new" AS assignment_mode
}
RETURN new_app.id AS app_id, assigned_cluster_id, assignment_mode;


// 5.1) Incremental batch mode for all unassigned applications (optional).
// Runs rule above for each Application with cluster_id IS NULL.
MATCH (new_app:Application)
WHERE new_app.cluster_id IS NULL
OPTIONAL MATCH (new_app)-[:LINKED]-(nbr:Application)
WHERE nbr.cluster_id IS NOT NULL
WITH new_app, nbr.cluster_id AS cid, count(*) AS votes
ORDER BY new_app.id, votes DESC, cid ASC
WITH new_app, collect({cid: cid, votes: votes}) AS ranked
CALL {
    WITH new_app, ranked
    WITH new_app, ranked[0] AS top_choice
    WHERE top_choice.cid IS NOT NULL
    SET new_app.cluster_id = top_choice.cid
    RETURN new_app.id AS app_id, new_app.cluster_id AS assigned_cluster_id, "existing" AS assignment_mode

    UNION

    WITH new_app, ranked
    WHERE size([x IN ranked WHERE x.cid IS NOT NULL]) = 0
    MATCH (a:Application)
    WITH new_app, coalesce(max(a.cluster_id), 0) + 1 AS new_cluster_id
    SET new_app.cluster_id = new_cluster_id
    RETURN new_app.id AS app_id, new_cluster_id AS assigned_cluster_id, "new" AS assignment_mode
}
RETURN app_id, assigned_cluster_id, assignment_mode
ORDER BY app_id;
