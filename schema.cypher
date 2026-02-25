// ============================================================
// Memgraph Schema: Anti-Fraud System for Online Lending
// ============================================================
//
// Architecture:
//   All borrower attributes are stored AS PROPERTIES on Application nodes.
//   Fraud links (LINKED edges) connect Applications that share attributes
//   (phone, email, document, etc.) directly — no intermediate attribute nodes.
//   This saves 5-10x memory compared to a hub-and-spoke attribute-node model.
//
// Graph model:
//   (Application)-[:LINKED {weight, shared_attrs}]->(Application)

// ------------------------------------------------------------
// 1. Constraints
// ------------------------------------------------------------

// Guarantees every application has a unique identifier.
// Memgraph enforces this at write time, preventing duplicate ingestion.
CREATE CONSTRAINT ON (a:Application) ASSERT a.id IS UNIQUE;

// ------------------------------------------------------------
// 2. Indexes — each one targets a specific query pattern
// ------------------------------------------------------------

// Fast lookup by primary key (exact match & existence checks).
CREATE INDEX ON :Application(id);

// Fraud-ring detection: find all applications that share the same phone.
CREATE INDEX ON :Application(phone);

// Fraud-ring detection: find all applications that share the same email.
CREATE INDEX ON :Application(email);

// Fraud-ring detection: find all applications that share the same document (passport / ID).
CREATE INDEX ON :Application(document);

// Fraud-ring detection: find all applications originating from the same IP address.
CREATE INDEX ON :Application(ip);

// Fraud-ring detection: find all applications linked to the same crypto / e-wallet.
CREATE INDEX ON :Application(wallet);

// Fraud-ring detection: find all applications with the same payout wallet.
CREATE INDEX ON :Application(payment_wallet);

// Fraud-ring detection: find all applications registered at the same physical address.
CREATE INDEX ON :Application(address);

// Workflow queries: filter by application lifecycle stage (pending / approved / rejected / fraud).
CREATE INDEX ON :Application(status);

// Cluster analysis: quickly retrieve all members of a fraud cluster.
CREATE INDEX ON :Application(cluster_id);

// Risk scoring: range queries for applications above a fraud-score threshold.
CREATE INDEX ON :Application(fraud_score);

// Temporal queries: filter / sort applications by submission time.
CREATE INDEX ON :Application(created_at);
