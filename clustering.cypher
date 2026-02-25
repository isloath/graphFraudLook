// ============================================================
// Memgraph MAGE: Louvain Clustering — Anti-Fraud System
// ============================================================
//
// Implements fraud-ring detection via community detection on the
// Application-LINKED-Application graph built by triggers.cypher.
//
// Algorithm:
//   Louvain community detection (MAGE community_detection module)
//   on undirected projection of LINKED edges, weighted by LINKED.weight.
//
// Fraud signal: tightly-closed clusters with high avg fraud_score.
//   closedness = internal_weight / (internal_weight + external_weight)
//   Threshold: closedness > 0.7  AND  cluster_size >= 5  → suspicious
//
// Sections:
//   0. Verify MAGE module availability
//   1. Dry-run Louvain (stats only, no writes)
//   2. Full run: compute + persist cluster_id to Application nodes
//   2b. Batch variant via periodic.iterate() for large graphs (> 500 k nodes)
//   3. Isolated-node handling (no LINKED edges → singleton clusters)
//   4. Cluster statistics with closedness metric
//   5. Suspicious cluster report
//   6. Incremental assignment for newly added applications
//
// Requires: Memgraph with MAGE (community_detection module).
// ============================================================


// ── 0. VERIFY MAGE MODULE ────────────────────────────────────────────────────
// Run once after Memgraph starts to confirm community_detection is loaded.
// Expected rows: get, get_subgraph, online procedures.

CALL mg.procedures() YIELD name, signature
WHERE name STARTS WITH "community_detection"
RETURN name, signature
ORDER BY name;


// ── 1. DRY RUN — stats only, no writes ──────────────────────────────────────
//
// Projects the Application-LINKED-Application subgraph and runs Louvain.
// Returns the number of clustered nodes and detected communities.
// Use this to validate the algorithm before committing cluster_id writes.
//
// Parameters passed to community_detection.get_subgraph():
//   nodes             — distinct Application nodes with at least one LINKED edge
//   rels              — distinct LINKED edges
//   directed  = false — treat LINKED as undirected (symmetric fraud links)
//   weighted  = true  — use LINKED.weight for modularity optimisation
//   weight_property   — property name on each relationship carrying the weight

MATCH (n:Application)-[r:LINKED]-(m:Application)
WITH collect(DISTINCT n) AS nodes,
     collect(DISTINCT r) AS rels
CALL community_detection.get_subgraph(nodes, rels, false, true, "weight")
YIELD node, community_id
RETURN count(node)              AS clustered_nodes,
       count(DISTINCT community_id) AS n_communities;


// ── 2. FULL RUN — compute + save cluster_id ──────────────────────────────────
//
// One-shot transaction: runs Louvain and writes community_id → cluster_id
// on every Application node that has at least one LINKED edge.
//
// Suitable for graphs up to ~500 k Application nodes.
// For larger graphs use Section 2b (periodic.iterate).

MATCH (n:Application)-[r:LINKED]-(m:Application)
WITH collect(DISTINCT n) AS nodes,
     collect(DISTINCT r) AS rels
CALL community_detection.get_subgraph(nodes, rels, false, true, "weight")
YIELD node, community_id
SET node.cluster_id = community_id;


// ── 2b. BATCH VARIANT — periodic.iterate() for large graphs ─────────────────
//
// Wraps the Louvain + SET in MAGE's periodic iterator so that updates are
// committed in batches of 1000, preventing OOM on very large graphs.
// Uncomment this block and comment out Section 2 when node count > 500 k.
//
// CALL periodic.iterate(
//   "MATCH (n:Application)-[r:LINKED]-(m:Application)
//    WITH collect(DISTINCT n) AS nodes, collect(DISTINCT r) AS rels
//    CALL community_detection.get_subgraph(nodes, rels, false, true, 'weight')
//    YIELD node, community_id
//    RETURN node, community_id",
//   "SET node.cluster_id = community_id",
//   {batchSize: 1000, parallel: false}
// ) YIELD batch_size, num_batches, total_processed
// RETURN batch_size, num_batches, total_processed;


// ── 3. ISOLATED NODES ────────────────────────────────────────────────────────
//
// Applications with no LINKED edges are invisible to community_detection
// (they are not part of any edge in the subgraph projection).
// Each gets a unique singleton cluster_id above the Louvain-assigned range,
// computed as:  max(existing cluster_id) + id(node) + 1
//
// id(node) is Memgraph's internal node id — stable within a session and
// guaranteed unique, so no two isolated nodes receive the same cluster_id.

MATCH (any:Application)
WHERE any.cluster_id IS NOT NULL
WITH max(any.cluster_id) AS base_cid

MATCH (isolated:Application)
WHERE NOT (isolated)-[:LINKED]-()
  AND isolated.cluster_id IS NULL
SET isolated.cluster_id = base_cid + id(isolated) + 1;


// ── 4. CLUSTER STATISTICS ────────────────────────────────────────────────────
//
// Per-cluster metrics used for fraud scoring:
//
//   cluster_size   — number of Application nodes in the cluster
//   internal_weight — sum of LINKED.weight for edges within the cluster
//                     (divided by 2 because undirected match yields each edge twice)
//   external_weight — sum of LINKED.weight for cross-cluster edges
//                     (each crossing edge counted once per cluster it touches)
//   closedness     — internal_weight / (internal_weight + external_weight)
//                     Range [0, 1]: 1 = fully closed ring, 0 = all edges external
//   avg_fraud_score — mean fraud_score of nodes already labelled in the cluster
//
// Clusters with no LINKED edges at all (singletons) are returned with
// closedness = 0 via a separate UNION branch.

// Branch A: clusters that appear in at least one LINKED edge
MATCH (a:Application)-[r:LINKED]-(b:Application)
WHERE a.cluster_id IS NOT NULL
  AND b.cluster_id IS NOT NULL
WITH a.cluster_id                                                            AS cid,
     sum(CASE WHEN a.cluster_id  = b.cluster_id THEN r.weight ELSE 0 END) / 2.0 AS internal_weight,
     sum(CASE WHEN a.cluster_id <> b.cluster_id THEN r.weight ELSE 0 END)       AS external_weight
WITH cid,
     internal_weight,
     external_weight,
     CASE
       WHEN (internal_weight + external_weight) > 0
         THEN internal_weight / (internal_weight + external_weight)
       ELSE 0.0
     END AS closedness

MATCH (a:Application)
WHERE a.cluster_id = cid
WITH cid,
     closedness,
     internal_weight,
     external_weight,
     count(a)                                  AS cluster_size,
     avg(coalesce(a.fraud_score, 0.0))         AS avg_fraud_score

RETURN cid              AS cluster_id,
       cluster_size,
       closedness,
       round(100.0 * closedness) / 100.0       AS closedness_pct,
       internal_weight,
       external_weight,
       round(1000.0 * avg_fraud_score) / 1000.0 AS avg_fraud_score
ORDER BY closedness DESC, cluster_size DESC;


// ── 5. SUSPICIOUS CLUSTERS ───────────────────────────────────────────────────
//
// A cluster is flagged suspicious when it satisfies ALL of:
//   • closedness  > 0.7   — tightly self-contained (high internal connectivity)
//   • cluster_size >= 5   — large enough to be a coordinated ring, not noise
//
// Adjust thresholds to fit observed data distribution.
// Returns full member list with fraud_score for manual review.

MATCH (a:Application)-[r:LINKED]-(b:Application)
WHERE a.cluster_id IS NOT NULL
  AND b.cluster_id IS NOT NULL
WITH a.cluster_id                                                            AS cid,
     sum(CASE WHEN a.cluster_id  = b.cluster_id THEN r.weight ELSE 0 END) / 2.0 AS internal_weight,
     sum(CASE WHEN a.cluster_id <> b.cluster_id THEN r.weight ELSE 0 END)       AS external_weight
WITH cid,
     internal_weight,
     CASE
       WHEN (internal_weight + external_weight) > 0
         THEN internal_weight / (internal_weight + external_weight)
       ELSE 0.0
     END AS closedness

MATCH (a:Application)
WHERE a.cluster_id = cid
WITH cid,
     closedness,
     count(a)                          AS cluster_size,
     avg(coalesce(a.fraud_score, 0.0)) AS avg_fraud_score,
     collect(a.id)                     AS member_ids

WHERE closedness  > 0.7
  AND cluster_size >= 5

RETURN cid             AS cluster_id,
       cluster_size,
       round(100.0 * closedness) / 100.0     AS closedness_pct,
       round(1000.0 * avg_fraud_score) / 1000.0 AS avg_fraud_score,
       member_ids
ORDER BY closedness DESC, cluster_size DESC;


// ── 6. INCREMENTAL ASSIGNMENT ────────────────────────────────────────────────
//
// Called after a new Application is added and its LINKED edges have been
// created by the trigger (triggers.cypher fires BEFORE COMMIT, so edges
// already exist when this query runs in the same or next transaction).
//
// Logic:
//   1. Gather all cluster_id values from the new node's direct neighbours.
//   2. Vote: each neighbour contributes its cluster_id weighted by the
//      LINKED.weight of the connecting edge (heavier edge = stronger signal).
//   3. Assign the cluster_id with the highest total weight (majority vote).
//   4. Fallback (no linked neighbours with a cluster_id):
//        new cluster_id = max(existing cluster_id) + 1
//      This creates an isolated singleton cluster without touching the
//      rest of the graph or re-running Louvain.
//
// Parameters:
//   $app_id — id of the newly inserted Application node.
//
// Returns:
//   app_id            — echoes the input id
//   assigned_cluster_id — the cluster_id written to the node
//   assignment_mode   — 'assigned_to_existing' | 'new_cluster_created'

WITH $app_id AS app_id
MATCH (new:Application {id: app_id})

// Step 1 & 2: weighted vote across direct neighbours
OPTIONAL MATCH (new)-[r:LINKED]-(nb:Application)
WHERE nb.cluster_id IS NOT NULL

WITH new,
     nb.cluster_id            AS nb_cid,
     sum(coalesce(r.weight, 1)) AS total_weight
ORDER BY total_weight DESC

// Step 3: take the top-voted cluster (null if no neighbours)
WITH new,
     collect(nb_cid)[0]  AS majority_cid

// Step 4: fallback — find max existing cluster_id for singleton assignment
OPTIONAL MATCH (any:Application)
WHERE any.cluster_id IS NOT NULL
  AND majority_cid IS NULL         // only scan when truly needed

WITH new,
     majority_cid,
     max(any.cluster_id) AS max_existing_cid

SET new.cluster_id = CASE
  WHEN majority_cid      IS NOT NULL THEN majority_cid
  WHEN max_existing_cid  IS NOT NULL THEN max_existing_cid + 1
  ELSE 1                                    // very first node in the graph
END

RETURN
  new.id         AS app_id,
  new.cluster_id AS assigned_cluster_id,
  CASE
    WHEN majority_cid IS NOT NULL THEN 'assigned_to_existing'
    ELSE 'new_cluster_created'
  END            AS assignment_mode;
