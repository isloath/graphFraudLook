// ============================================================
// Memgraph Triggers: Auto-linking Applications by shared attributes
// ============================================================
//
// When a new Application vertex is created, this trigger:
//   1. Finds all existing Applications (within 2-year window)
//      that share ANY attribute with the new one
//   2. Computes the list of shared attributes and its count (weight)
//   3. Creates a LINKED edge (or updates an existing one)
//
// Depends on indexes from schema.cypher for fast attribute lookups.
//
// Uses BEFORE COMMIT — the LINKED edges are created atomically
// with the Application vertex in the same transaction.
// Trade-off: if linking fails, the entire transaction rolls back.
// Switch to AFTER COMMIT if you prefer fire-and-forget semantics.

CREATE TRIGGER link_new_applications
ON () CREATE BEFORE COMMIT EXECUTE

  // -- Step 1: iterate over newly created vertices -----------------
  UNWIND createdVertices AS new
  WITH new
  WHERE new:Application

  // -- Step 2: define a 2-year look-back window --------------------
  //    Older applications are unlikely to belong to the same fraud
  //    ring and skipping them keeps the MATCH fast.
  WITH new, localDateTime() - duration({day: 730}) AS cutoff

  // -- Step 3: find candidates that share at least one attribute ---
  //    The OR ensures we only visit rows with at least one match,
  //    so weight is guaranteed >= 1 — no extra filter needed.
  MATCH (existing:Application)
  WHERE existing.id <> new.id
    AND existing.created_at > cutoff
    AND (
         (new.phone          IS NOT NULL AND existing.phone          = new.phone)
      OR (new.email          IS NOT NULL AND existing.email          = new.email)
      OR (new.document       IS NOT NULL AND existing.document       = new.document)
      OR (new.ip             IS NOT NULL AND existing.ip             = new.ip)
      OR (new.wallet         IS NOT NULL AND existing.wallet         = new.wallet)
      OR (new.payment_wallet IS NOT NULL AND existing.payment_wallet = new.payment_wallet)
      OR (new.address        IS NOT NULL AND existing.address        = new.address)
    )

  // -- Step 4: compute shared attributes in a single pass ----------
  //    CASE without ELSE returns NULL; the list comprehension
  //    filters NULLs out, leaving only the names that actually match.
  WITH new, existing,
       [attr IN [
         CASE WHEN new.phone          IS NOT NULL AND existing.phone          = new.phone          THEN 'phone'          END,
         CASE WHEN new.email          IS NOT NULL AND existing.email          = new.email          THEN 'email'          END,
         CASE WHEN new.document       IS NOT NULL AND existing.document       = new.document       THEN 'document'       END,
         CASE WHEN new.ip             IS NOT NULL AND existing.ip             = new.ip             THEN 'ip'             END,
         CASE WHEN new.wallet         IS NOT NULL AND existing.wallet         = new.wallet         THEN 'wallet'         END,
         CASE WHEN new.payment_wallet IS NOT NULL AND existing.payment_wallet = new.payment_wallet THEN 'payment_wallet' END,
         CASE WHEN new.address        IS NOT NULL AND existing.address        = new.address        THEN 'address'        END
       ] WHERE attr IS NOT NULL] AS shared_attrs

  WITH new, existing, shared_attrs, size(shared_attrs) AS weight

  // -- Step 5: create or update the LINKED edge --------------------
  //    MERGE prevents duplicate edges if the trigger fires twice
  //    for the same pair (e.g., batch inserts).
  //    ON CREATE — first time: set all properties.
  //    ON MATCH  — edge exists: refresh weight & shared_attrs.
  MERGE (new)-[r:LINKED]->(existing)
  ON CREATE SET r.weight       = weight,
                r.shared_attrs = shared_attrs,
                r.created_at   = localDateTime()
  ON MATCH  SET r.weight       = weight,
                r.shared_attrs = shared_attrs;
