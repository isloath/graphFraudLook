// ============================================================
// Memgraph Queries: Anti-Fraud System for Online Lending
// ============================================================
//
// All queries use:
//   - Parameterized inputs ($param) — no string concatenation
//   - LIMIT to cap result sets
//   - Indexed property as the entry point in MATCH
//   - Comments with expected execution time
//
// Depends on indexes defined in schema.cypher.
// ============================================================


// ------------------------------------------------------------
// 1. find_by_phone
//    Find applications by phone number.
//    Entry point: indexed Application.phone
//    Parameters: $phone, $limit
//    Expected time: <10ms (single index lookup)
// ------------------------------------------------------------

MATCH (a:Application)
WHERE a.phone = $phone
RETURN a.id          AS id,
       a.created_at  AS created_at,
       a.email       AS email,
       a.document    AS document,
       a.ip          AS ip,
       a.amount      AS amount,
       a.status      AS status,
       a.cluster_id  AS cluster_id,
       a.fraud_score AS fraud_score
ORDER BY a.created_at DESC
LIMIT $limit;


// ------------------------------------------------------------
// 2. get_neighbors
//    Neighbors of an application through LINKED edges
//    up to a configurable depth (1-3 hops).
//    Entry point: indexed Application.id
//    Parameters: $app_id, $depth (1-3), $limit
//    Expected time: <10ms depth=1, <50ms depth=2, <200ms depth=3
// ------------------------------------------------------------

// -- 2a. depth = 1
MATCH (src:Application {id: $app_id})-[r:LINKED]-(neighbor:Application)
RETURN neighbor.id          AS id,
       neighbor.status      AS status,
       neighbor.fraud_score AS fraud_score,
       neighbor.cluster_id  AS cluster_id,
       r.weight             AS link_weight,
       r.shared_attrs       AS shared_attrs
ORDER BY r.weight DESC
LIMIT $limit;

// -- 2b. depth = 2
MATCH path = (src:Application {id: $app_id})-[:LINKED*1..2]-(neighbor:Application)
WHERE neighbor.id <> $app_id
WITH DISTINCT neighbor,
     min(size(relationships(path))) AS hops
RETURN neighbor.id          AS id,
       neighbor.status      AS status,
       neighbor.fraud_score AS fraud_score,
       neighbor.cluster_id  AS cluster_id,
       hops
ORDER BY hops, neighbor.fraud_score DESC
LIMIT $limit;

// -- 2c. depth = 3
MATCH path = (src:Application {id: $app_id})-[:LINKED*1..3]-(neighbor:Application)
WHERE neighbor.id <> $app_id
WITH DISTINCT neighbor,
     min(size(relationships(path))) AS hops
RETURN neighbor.id          AS id,
       neighbor.status      AS status,
       neighbor.fraud_score AS fraud_score,
       neighbor.cluster_id  AS cluster_id,
       hops
ORDER BY hops, neighbor.fraud_score DESC
LIMIT $limit;


// ------------------------------------------------------------
// 3. get_shared_applications
//    Applications linked to a given app with at least
//    $min_shared common attributes (via LINKED.weight).
//    Entry point: indexed Application.id
//    Parameters: $app_id, $min_shared, $limit
//    Returns: app, weight, shared_attrs
//    Expected time: <20ms
// ------------------------------------------------------------

MATCH (src:Application {id: $app_id})-[r:LINKED]-(app:Application)
WHERE r.weight >= $min_shared
RETURN app.id          AS id,
       app.phone       AS phone,
       app.email       AS email,
       app.document    AS document,
       app.status      AS status,
       app.fraud_score AS fraud_score,
       r.weight        AS weight,
       r.shared_attrs  AS shared_attrs
ORDER BY r.weight DESC, app.fraud_score DESC
LIMIT $limit;


// ------------------------------------------------------------
// 4. get_cluster_members
//    All applications belonging to a fraud cluster,
//    with SKIP/LIMIT pagination.
//    Entry point: indexed Application.cluster_id
//    Parameters: $cluster_id, $limit, $offset
//    Expected time: <15ms
// ------------------------------------------------------------

MATCH (a:Application)
WHERE a.cluster_id = $cluster_id
RETURN a.id          AS id,
       a.created_at  AS created_at,
       a.phone       AS phone,
       a.email       AS email,
       a.document    AS document,
       a.amount      AS amount,
       a.status      AS status,
       a.fraud_score AS fraud_score
ORDER BY a.created_at DESC
SKIP $offset
LIMIT $limit;


// ------------------------------------------------------------
// 5. get_hot_attributes
//    Attributes (phone / email / document / ip / wallet /
//    payment_wallet / address) used by more than $min_uses
//    applications within a time window.
//    Entry point: indexed Application.created_at (range scan)
//    Parameters: $attr_type, $min_uses, $since_date, $limit
//    Expected time: <100ms (full scan with aggregation)
//
//    $attr_type must be one of:
//      'phone', 'email', 'document', 'ip',
//      'wallet', 'payment_wallet', 'address'
// ------------------------------------------------------------

// -- 5a. phone
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.phone IS NOT NULL
WITH a.phone AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'phone'   AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5b. email
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.email IS NOT NULL
WITH a.email AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'email'   AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5c. document
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.document IS NOT NULL
WITH a.document AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'document' AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5d. ip
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.ip IS NOT NULL
WITH a.ip AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'ip'      AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5e. wallet
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.wallet IS NOT NULL
WITH a.wallet AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'wallet'  AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5f. payment_wallet
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.payment_wallet IS NOT NULL
WITH a.payment_wallet AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'payment_wallet' AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;

// -- 5g. address
MATCH (a:Application)
WHERE a.created_at >= localDateTime($since_date)
  AND a.address IS NOT NULL
WITH a.address AS attr_value, count(a) AS use_count
WHERE use_count >= $min_uses
RETURN 'address' AS attr_type,
       attr_value,
       use_count
ORDER BY use_count DESC
LIMIT $limit;


// ------------------------------------------------------------
// 6. get_application_with_context
//    Single query: application properties + neighbor count +
//    count of fraud-flagged neighbors + cluster info.
//    Entry point: indexed Application.id
//    Parameters: $app_id
//    Expected time: <30ms
// ------------------------------------------------------------

MATCH (app:Application {id: $app_id})
OPTIONAL MATCH (app)-[r:LINKED]-(neighbor:Application)
WITH app,
     count(neighbor)                                          AS neighbor_count,
     sum(CASE WHEN neighbor.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_neighbor_count,
     max(r.weight)                                            AS max_link_weight,
     avg(r.weight)                                            AS avg_link_weight
RETURN app.id              AS id,
       app.created_at      AS created_at,
       app.phone           AS phone,
       app.email           AS email,
       app.document        AS document,
       app.ip              AS ip,
       app.wallet          AS wallet,
       app.payment_wallet  AS payment_wallet,
       app.address         AS address,
       app.amount          AS amount,
       app.status          AS status,
       app.cluster_id      AS cluster_id,
       app.fraud_score     AS fraud_score,
       neighbor_count,
       fraud_neighbor_count,
       max_link_weight,
       avg_link_weight;


// ------------------------------------------------------------
// 7. get_recent_applications_velocity
//    Applications created after $since, ordered by the number
//    of LINKED edges they formed — for velocity monitoring.
//    Entry point: indexed Application.created_at (range scan)
//    Parameters: $since, $limit
//    Expected time: <100ms
// ------------------------------------------------------------

MATCH (a:Application)
WHERE a.created_at >= localDateTime($since)
OPTIONAL MATCH (a)-[r:LINKED]-(neighbor:Application)
WITH a,
     count(r)                                                    AS link_count,
     sum(CASE WHEN neighbor.status = 'fraud' THEN 1 ELSE 0 END) AS fraud_link_count
RETURN a.id          AS id,
       a.created_at  AS created_at,
       a.phone       AS phone,
       a.email       AS email,
       a.amount      AS amount,
       a.status      AS status,
       a.fraud_score AS fraud_score,
       link_count,
       fraud_link_count
ORDER BY link_count DESC
LIMIT $limit;
