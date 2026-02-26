// ============================================================
// Memgraph Triggers: Auto-linking Applications by shared attributes
// ============================================================
//
// When a new Application vertex is created, this trigger:
//   1. Finds all existing Applications (within a 2-year window)
//      that share ANY attribute with the new one.
//   2. Computes the weighted sum of shared attributes:
//        document / wallet / payment_wallet → 5 pts each (strong identity)
//        phone                              → 3 pts       (moderate signal)
//        email                              → 2 pts       (weak signal)
//        address / ip                       → 1 pt each   (ambient signal)
//   3. Creates a LINKED edge (or updates the existing one) with:
//        weight       = sum of weights for all matched attributes
//        shared_attrs = list of matched attribute names
//
// Optimisation: both shared_attrs and weight are derived in a SINGLE
// WITH clause via parallel CASE WHEN expressions — Memgraph evaluates
// each (new, existing) pair exactly once.
//
// Depends on indexes from schema.cypher for fast attribute lookups.
//
// Uses BEFORE COMMIT — the LINKED edges are created atomically with
// the Application vertex in the same transaction.
// Trade-off: if linking fails, the entire transaction rolls back.
// Switch to AFTER COMMIT for fire-and-forget semantics.

CREATE TRIGGER link_new_applications
ON () CREATE BEFORE COMMIT EXECUTE

  // ── Step 1: iterate over newly created vertices ─────────────────
  UNWIND createdVertices AS new
  WITH new
  WHERE new:Application

  // ── Step 2: define a 2-year look-back window ─────────────────────
  //    Applications older than 730 days are unlikely to belong to the
  //    same fraud ring; skipping them keeps the candidate scan fast.
  WITH new, localDateTime() - duration({day: 730}) AS cutoff

  // ── Step 3: find candidates that share at least one attribute ────
  //    The OR filter guarantees weight >= 1 for every returned row,
  //    so no extra weight > 0 check is needed here.
  MATCH (existing:Application)
  WHERE existing.id <> new.id
    AND existing.created_at > cutoff
    AND (
         (new.document       IS NOT NULL AND existing.document       = new.document)
      OR (new.wallet         IS NOT NULL AND existing.wallet         = new.wallet)
      OR (new.payment_wallet IS NOT NULL AND existing.payment_wallet = new.payment_wallet)
      OR (new.phone          IS NOT NULL AND existing.phone          = new.phone)
      OR (new.email          IS NOT NULL AND existing.email          = new.email)
      OR (new.address        IS NOT NULL AND existing.address        = new.address)
      OR (new.ip             IS NOT NULL AND existing.ip             = new.ip)
    )

  // ── Step 4: compute shared_attrs and weight in one pass ──────────
  //
  //    shared_attrs — list of matched attribute names, built via a
  //      list comprehension that filters out NULL entries produced by
  //      CASE expressions where the condition is false.
  //
  //    weight — arithmetic sum of per-attribute scores evaluated in
  //      parallel CASE WHEN expressions (no second scan, no subquery).
  //
  //    Attribute weights mirror the yaml config in src/config/attribute_weights.yaml:
  //      document / wallet / payment_wallet → 5
  //      phone                              → 3
  //      email                              → 2
  //      address / ip                       → 1
  WITH new, existing,

       [attr IN [
         CASE WHEN new.document       IS NOT NULL AND existing.document       = new.document       THEN 'document'       END,
         CASE WHEN new.wallet         IS NOT NULL AND existing.wallet         = new.wallet         THEN 'wallet'         END,
         CASE WHEN new.payment_wallet IS NOT NULL AND existing.payment_wallet = new.payment_wallet THEN 'payment_wallet' END,
         CASE WHEN new.phone          IS NOT NULL AND existing.phone          = new.phone          THEN 'phone'          END,
         CASE WHEN new.email          IS NOT NULL AND existing.email          = new.email          THEN 'email'          END,
         CASE WHEN new.address        IS NOT NULL AND existing.address        = new.address        THEN 'address'        END,
         CASE WHEN new.ip             IS NOT NULL AND existing.ip             = new.ip             THEN 'ip'             END
       ] WHERE attr IS NOT NULL] AS shared_attrs,

       (  CASE WHEN new.document       IS NOT NULL AND existing.document       = new.document       THEN 5 ELSE 0 END
        + CASE WHEN new.wallet         IS NOT NULL AND existing.wallet         = new.wallet         THEN 5 ELSE 0 END
        + CASE WHEN new.payment_wallet IS NOT NULL AND existing.payment_wallet = new.payment_wallet THEN 5 ELSE 0 END
        + CASE WHEN new.phone          IS NOT NULL AND existing.phone          = new.phone          THEN 3 ELSE 0 END
        + CASE WHEN new.email          IS NOT NULL AND existing.email          = new.email          THEN 2 ELSE 0 END
        + CASE WHEN new.address        IS NOT NULL AND existing.address        = new.address        THEN 1 ELSE 0 END
        + CASE WHEN new.ip             IS NOT NULL AND existing.ip             = new.ip             THEN 1 ELSE 0 END
       ) AS weight

  // ── Step 5: safety guard — skip zero-weight edges ────────────────
  //    Redundant after the Step 3 OR filter, but guards against
  //    future schema changes that might introduce null-safe comparisons.
  WITH new, existing, shared_attrs, weight
  WHERE weight > 0

  // ── Step 6: create or update the LINKED edge ──────────────────────
  //    MERGE prevents duplicate edges on concurrent batch inserts.
  //    ON CREATE — first link: persist all properties.
  //    ON MATCH  — edge already exists: refresh weight and shared_attrs
  //                (shared attributes may change if node properties are updated).
  MERGE (new)-[r:LINKED]->(existing)
  ON CREATE SET r.weight       = weight,
                r.shared_attrs = shared_attrs,
                r.created_at   = localDateTime()
  ON MATCH  SET r.weight       = weight,
                r.shared_attrs = shared_attrs;
