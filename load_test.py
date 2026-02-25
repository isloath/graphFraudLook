#!/usr/bin/env python3
"""
load_test.py — Full pipeline smoke-test for the anti-fraud Memgraph graph.

Run:
    python load_test.py
    MEMGRAPH_HOST=localhost MEMGRAPH_PORT=7687 python load_test.py

Sections:
  0.  Config   — paths, load WEIGHTS from src/config/attribute_weights.yaml
  1.  Schema   — wipe + apply schema.cypher + schema/triggers.cypher
  2.  Trigger smoke-test — insert 4 apps, assert edge weights before bulk load
  3.  Generate — honest / fraud / gray clusters (~19 300 apps)
  4.  Bulk load nodes  (trigger dropped → fast)
  5.  Bulk load LINKED edges with correct weighted sums
  6.  Re-create trigger for future single-row inserts
  7.  Weight integrity check — sample 1 000 edges, verify weight == sum(WEIGHTS)
  8.  Graph counts + weight histogram
  9.  Louvain clustering (requires Memgraph MAGE)
 10.  Cluster stats + suspicious cluster report
"""

import os
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# 0. Config
# ─────────────────────────────────────────────────────────────────────────────

ROOT          = Path(__file__).resolve().parent
SCHEMA_FILE   = ROOT / "schema.cypher"
TRIGGERS_FILE = ROOT / "schema" / "triggers.cypher"   # canonical location

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load attribute weights from YAML; fall back to hardcoded table if pyyaml
# is not installed or the file is missing.
_WEIGHTS_FILE = ROOT / "src" / "config" / "attribute_weights.yaml"
try:
    import yaml                                        # pip install pyyaml
    with _WEIGHTS_FILE.open(encoding="utf-8") as _f:
        WEIGHTS: dict[str, int] = yaml.safe_load(_f)["weights"]
    print(f"Weights loaded from {_WEIGHTS_FILE.relative_to(ROOT)}")
except Exception as _err:
    WEIGHTS = {
        "document": 5, "wallet": 5, "payment_wallet": 5,
        "phone": 3, "email": 2, "address": 1, "ip": 1,
    }
    print(f"Warning: YAML load failed ({_err}). Using hardcoded weights.")

print(f"WEIGHTS: {WEIGHTS}")
LINK_ATTRS = list(WEIGHTS.keys())   # order mirrors triggers.cypher

SEED       = 42
BATCH_SIZE = 500
EDGE_BATCH = 2_000

# ─────────────────────────────────────────────────────────────────────────────
# 1. Connect
# ─────────────────────────────────────────────────────────────────────────────

from graph_client import FraudGraphClient

HOST   = os.environ.get("MEMGRAPH_HOST", "localhost")
PORT   = int(os.environ.get("MEMGRAPH_PORT", "7687"))
client = FraudGraphClient(host=HOST, port=PORT)
print(f"Connected to Memgraph at {HOST}:{PORT}\n")

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_cypher(path: Path) -> list[str]:
    """Strip // comments and split on ';' into individual statements."""
    lines = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s.startswith("//"):
            continue
        idx = raw.find("//")
        lines.append(raw[:idx] if idx != -1 else raw)
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def edge_weight(attrs: list[str]) -> int:
    """Weighted sum for a list of shared attribute names.

    Mirrors the CASE WHEN arithmetic in schema/triggers.cypher:
      ["phone", "document"] -> 3 + 5 = 8
    """
    return sum(WEIGHTS.get(a, 0) for a in attrs)


def apply_schema_and_triggers() -> None:
    """Wipe the graph, apply indexes, install the LINKED trigger."""
    client.execute_raw("MATCH (n) DETACH DELETE n", {})
    try:
        client.execute_raw("DROP TRIGGER link_new_applications", {})
    except Exception:
        pass
    for stmt in parse_cypher(SCHEMA_FILE):
        try:
            client.execute_raw(stmt, {})
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
    for stmt in parse_cypher(TRIGGERS_FILE):
        try:
            client.execute_raw(stmt, {})
        except Exception as e:
            print(f"  [warn] trigger stmt skipped: {e}")
    print(f"Schema applied  ({SCHEMA_FILE.name})")
    print(f"Trigger applied ({TRIGGERS_FILE.relative_to(ROOT)})")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Trigger smoke-test
# ─────────────────────────────────────────────────────────────────────────────

def _run_smoke_test() -> None:
    """
    Insert 4 applications through the trigger and assert edge weights.

    Test matrix (all share phone='+70000000001'):
      A  (anchor)
      B  phone only          -> weight = 3
      C  phone + document    -> weight = 3 + 5 = 8
      D  phone + email       -> weight = 3 + 2 = 5
    """
    print("── Trigger smoke-test ───────────────────────────────────────────────")

    ts = "2025-01-01T00:00:00"
    apps = [
        {"id": "_s_A", "created_at": ts, "phone": "+70000000001",
         "email": "a@t.com", "document": "DOC-A", "amount": 1.0, "status": "pending"},
        {"id": "_s_B", "created_at": ts, "phone": "+70000000001",
         "email": "b@t.com", "document": "DOC-B", "amount": 2.0, "status": "pending"},
        {"id": "_s_C", "created_at": ts, "phone": "+70000000001",
         "email": "c@t.com", "document": "DOC-A", "amount": 3.0, "status": "pending"},
        {"id": "_s_D", "created_at": ts, "phone": "+70000000001",
         "email": "a@t.com", "document": "DOC-D", "amount": 4.0, "status": "pending"},
    ]
    for a in apps:
        client.add_application(a)

    checks = [
        ("_s_B", "_s_A", {"phone"},           edge_weight(["phone"])),
        ("_s_C", "_s_A", {"phone", "document"}, edge_weight(["phone", "document"])),
        ("_s_D", "_s_A", {"phone", "email"},    edge_weight(["phone", "email"])),
    ]

    all_ok = True
    for src, dst, exp_attrs, exp_w in checks:
        row = client.execute_raw(
            "MATCH (a:Application {id:$s})-[r:LINKED]->(b:Application {id:$d}) "
            "RETURN r.weight AS w, r.shared_attrs AS sa",
            {"s": src, "d": dst},
        )
        if not row:
            print(f"  FAIL  {src}->{dst}: no edge created")
            all_ok = False
            continue
        got_w, got_attrs = row[0]["w"], set(row[0]["sa"] or [])
        ok = got_w == exp_w and got_attrs == exp_attrs
        icon = "✓" if ok else "✗"
        print(f"  {icon}  {src}->{dst}  "
              f"weight={got_w} (exp {exp_w})  "
              f"shared={got_attrs} (exp {exp_attrs})")
        if not ok:
            all_ok = False

    print("  Result:", "ALL PASSED" if all_ok else "FAILURES — check schema/triggers.cypher")
    client.execute_raw(
        "MATCH (a:Application) WHERE a.id STARTS WITH '_s_' DETACH DELETE a", {}
    )
    print()


apply_schema_and_triggers()
_run_smoke_test()

# ─────────────────────────────────────────────────────────────────────────────
# 3. Generate data
# ─────────────────────────────────────────────────────────────────────────────

from generate_test_data import AppGenerator

try:
    from generate_test_data import get_link_pairs as _glp

    def get_link_pairs(apps: list[dict]) -> list[dict]:   # noqa: F811
        """Wrap generator's get_link_pairs, adding weighted 'weight' field."""
        pairs = _glp(apps)
        for p in pairs:
            p["weight"] = edge_weight(p.get("attrs", []))
        return pairs

    print("get_link_pairs: using generate_test_data (with weight wrapper)")
except ImportError:
    def get_link_pairs(apps: list[dict]) -> list[dict]:
        """Local fallback — returns weighted pairs."""
        inv: dict[tuple, set] = defaultdict(set)
        for app in apps:
            for attr in LINK_ATTRS:
                val = app.get(attr)
                if val is not None:
                    inv[(attr, val)].add(app["id"])
        pair_attrs: dict[tuple, set] = defaultdict(set)
        for (attr, _), ids in inv.items():
            if len(ids) < 2:
                continue
            for a, b in combinations(sorted(ids), 2):
                pair_attrs[(a, b)].add(attr)
        return [
            {"a": a, "b": b, "attrs": list(attrs), "weight": edge_weight(list(attrs))}
            for (a, b), attrs in pair_attrs.items()
        ]
    print("get_link_pairs: using local weighted fallback")

honest_count = 17_000
fraud_sizes  = [200, 300, 400, 500, 600]
gray_sizes   = [150, 150]

gen = AppGenerator(seed=SEED)
print("\nGenerating applications...")

honest = gen.generate_honest(honest_count)
AppGenerator.inject_overlaps(honest)
print(f"  Honest : {len(honest)}")

fraud: list[dict] = []
for i, sz in enumerate(fraud_sizes):
    fraud.extend(gen.generate_fraud_cluster(i + 1, sz))
print(f"  Fraud  : {len(fraud)}  (clusters: {fraud_sizes})")

honest_phones = [a["phone"] for a in honest[:50]]
gray: list[dict] = []
for i, sz in enumerate(gray_sizes):
    gray.extend(gen.generate_gray_cluster(len(fraud_sizes) + i + 1, sz, honest_phones))
print(f"  Gray   : {len(gray)}")

all_apps = honest + fraud + gray
print(f"  TOTAL  : {len(all_apps)}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Bulk load nodes (trigger dropped for speed)
# ─────────────────────────────────────────────────────────────────────────────

try:
    client.execute_raw("DROP TRIGGER link_new_applications", {})
except Exception:
    pass

_NODE_Q = """
UNWIND $batch AS row
CREATE (a:Application)
SET a += row
SET a.created_at = CASE
    WHEN row.created_at IS NOT NULL THEN localDateTime(row.created_at)
    ELSE localDateTime()
END
"""

print("\nBulk-loading nodes...")
total = 0
for i in range(0, len(all_apps), BATCH_SIZE):
    client.execute_raw(_NODE_Q, {"batch": all_apps[i : i + BATCH_SIZE]})
    total += len(all_apps[i : i + BATCH_SIZE])
    if total % 5_000 == 0 or total == len(all_apps):
        print(f"  {total}/{len(all_apps)}")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Bulk load LINKED edges — weighted sum, not size(attrs)
# ─────────────────────────────────────────────────────────────────────────────

print("\nComputing LINKED edge pairs...")
pairs = get_link_pairs(all_apps)

wdist = Counter(p["weight"] for p in pairs)
print(f"  Total pairs : {len(pairs)}")
print("  Weight distribution:")
for w in sorted(wdist, reverse=True):
    bar = "█" * min(wdist[w] // max(1, len(pairs) // 50), 50)
    print(f"    w={w:>3}: {wdist[w]:>6}  {bar}")

# Key fix vs original script: use e.weight (precomputed weighted sum) not size(e.attrs).
# Pairs are already deduplicated by combinations(sorted(ids), 2).
_EDGE_Q = """
UNWIND $edges AS e
MATCH (a:Application {id: e.a}), (b:Application {id: e.b})
MERGE (a)-[r:LINKED]->(b)
ON CREATE SET r.weight       = e.weight,
              r.shared_attrs = e.attrs,
              r.created_at   = localDateTime()
ON MATCH  SET r.weight       = e.weight,
              r.shared_attrs = e.attrs
"""

print("\nBulk-loading LINKED edges...")
done = 0
for i in range(0, len(pairs), EDGE_BATCH):
    client.execute_raw(_EDGE_Q, {"edges": pairs[i : i + EDGE_BATCH]})
    done = min(i + EDGE_BATCH, len(pairs))
    if done % 10_000 < EDGE_BATCH or done == len(pairs):
        print(f"  {done}/{len(pairs)}")

# ─────────────────────────────────────────────────────────────────────────────
# 6. Re-create trigger
# ─────────────────────────────────────────────────────────────────────────────

print("\nRe-creating trigger...")
for stmt in parse_cypher(TRIGGERS_FILE):
    try:
        client.execute_raw(stmt, {})
    except Exception as e:
        print(f"  [warn] {e}")
print("Trigger active for future single-row inserts.")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Weight integrity check
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Weight integrity check (1 000 sampled edges) ─────────────────────────")
sample = client.execute_raw(
    "MATCH ()-[r:LINKED]->() WITH r LIMIT 1000 "
    "RETURN r.shared_attrs AS attrs, r.weight AS weight",
    {},
)
bad = [r for r in sample if r["weight"] != edge_weight(r["attrs"] or [])]
if bad:
    print(f"  ✗  Mismatch in {len(bad)}/{len(sample)} edges (first 3):")
    for r in bad[:3]:
        print(f"     attrs={r['attrs']}  got={r['weight']}  "
              f"expected={edge_weight(r['attrs'] or [])}")
else:
    print(f"  ✓  All {len(sample)} sampled edges carry correct weighted sums.")

# ─────────────────────────────────────────────────────────────────────────────
# 8. Graph counts + weight histogram
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Graph summary ────────────────────────────────────────────────────────")
n_nodes  = client.execute_raw("MATCH (a:Application) RETURN count(a) AS n", {})[0]["n"]
n_edges2 = client.execute_raw("MATCH ()-[r:LINKED]-() RETURN count(r) AS n", {})[0]["n"]
n_edges  = n_edges2 // 2
max_w    = client.execute_raw("MATCH ()-[r:LINKED]->() RETURN max(r.weight) AS m", {})[0]["m"] or 0
avg_w    = client.execute_raw("MATCH ()-[r:LINKED]->() RETURN avg(r.weight) AS a", {})[0]["a"] or 0

print(f"  Applications         : {n_nodes:>8,}")
print(f"  LINKED edges (unique): {n_edges:>8,}")
print(f"  Max edge weight      : {max_w:>8}  (max possible: {sum(WEIGHTS.values())})")
print(f"  Avg edge weight      : {avg_w:>11.2f}")

hist = client.execute_raw(
    "MATCH ()-[r:LINKED]->() RETURN r.weight AS w, count(*) AS n ORDER BY w DESC LIMIT 15",
    {},
)
print("\n  Weight histogram (from graph):")
for row in hist:
    bar = "█" * min(row["n"] // max(1, n_edges // 50), 50)
    print(f"    w={row['w']:>3}: {row['n']:>6}  {bar}")

# ─────────────────────────────────────────────────────────────────────────────
# 9. Louvain clustering
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Louvain clustering (MAGE) ────────────────────────────────────────────")
# Use community_detection.get (streaming) instead of get_subgraph (materialises
# ALL nodes+rels into RAM before calling MAGE).  get_subgraph causes:
#   "Memory limit exceeded! Attempting to allocate 4 GiB…"
# because it builds two full in-memory copies (Cypher collect() + MAGE copy).
# community_detection.get streams directly from the graph — O(1) extra RAM.
try:
    client.execute_raw(
        # Signature: community_detection.get(weight_property STRING,
        #                                     directed BOOL, weighted BOOL)
        # weight_property is position 0 — must be STRING, not bool.
        # No node_label/rel_type filter args in this MAGE version.
        """
        CALL community_detection.get("weight", false, true)
        YIELD node, community_id
        SET node.cluster_id = community_id
        """,
        {},
    )
    # Isolated nodes (no LINKED edges) → unique singleton cluster_id
    client.execute_raw(
        """
        MATCH (any:Application) WHERE any.cluster_id IS NOT NULL
        WITH max(any.cluster_id) AS base
        MATCH (iso:Application)
        WHERE NOT (iso)-[:LINKED]-() AND iso.cluster_id IS NULL
        SET iso.cluster_id = base + id(iso) + 1
        """,
        {},
    )
    n_cl = client.execute_raw(
        "MATCH (a:Application) WHERE a.cluster_id IS NOT NULL RETURN count(a) AS n", {}
    )[0]["n"]
    print(f"  Nodes with cluster_id: {n_cl}")
except Exception as e:
    print(f"  Louvain unavailable (MAGE required): {e}")
    print("  Skipping cluster analytics.")
    sys.exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# 10. Cluster stats + suspicious cluster report
# ─────────────────────────────────────────────────────────────────────────────

HDR = f"  {'cluster_id':>12}  {'size':>6}  {'closedness':>10}  " \
      f"{'fraud_ratio':>11}  {'avg_weight':>10}"
SEP = "  " + "─" * 56

all_stats    = sorted(client.get_all_cluster_stats(min_size=5),
                      key=lambda s: s["size"], reverse=True)

print(f"\n── Top 10 clusters by size  ({len(all_stats)} total with size>=5) ──────────")
print(HDR); print(SEP)
for s in all_stats[:10]:
    print(f"  {s['cluster_id']:>12}  {s['size']:>6}  "
          f"{s['closedness']:>10.3f}  {s['fraud_ratio']:>11.2f}  "
          f"{s['avg_internal_weight']:>10.1f}")

suspicious = client.get_suspicious_clusters(closedness_threshold=0.7, min_size=10)
print(f"\n── Suspicious clusters (closedness>0.7, size>=10) — {len(suspicious)} found ──")
if suspicious:
    print(HDR); print(SEP)
    for c in suspicious[:15]:
        print(f"  {c['cluster_id']:>12}  {c['size']:>6}  "
              f"{c['closedness']:>10.3f}  {c['fraud_ratio']:>11.2f}  "
              f"{c['avg_internal_weight']:>10.1f}")

    top = suspicious[0]
    stats = client.get_cluster_stats(top["cluster_id"])
    print(f"\n  Detail — cluster {top['cluster_id']} (highest closedness):")
    for k, v in stats.items():
        print(f"    {k:<26}: {v:.3f}" if isinstance(v, float) else f"    {k:<26}: {v}")

    top3 = [c["cluster_id"] for c in suspicious[:3]]
    print(f"\n  Memgraph Lab — top 3 suspicious:")
    print(f"    MATCH (a:Application)-[r:LINKED]-(b:Application)")
    print(f"    WHERE a.cluster_id IN {top3} AND b.cluster_id IN {top3}")
    print(f"    RETURN a, r, b LIMIT 500")
else:
    print("  None found — adjust thresholds or generate more fraud clusters.")

print("\nDone.")
