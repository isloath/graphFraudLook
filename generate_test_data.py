#!/usr/bin/env python3
"""Generate test dataset for Memgraph anti-fraud online lending system.

Creates Application nodes across three categories:
  - Honest: random unique attributes, rare accidental overlaps
  - Fraud clusters: heavily reused phones/documents/IPs/wallets
  - Gray-zone clusters: partial reuse + cross-links to honest apps

Output: Cypher UNWIND batches for bulk loading into Memgraph.

Usage:
    python generate_test_data.py --honest=10000 --fraud-clusters=5 --output=cypher
    python generate_test_data.py --honest=5000 --output-file=data.cypher
"""

import argparse
import random
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import combinations

try:
    from faker import Faker
except ImportError:
    print("Install required dependency: pip install faker", file=sys.stderr)
    sys.exit(1)

# ── constants ─────────────────────────────────────────────────────────────────

LINK_ATTRS = ["phone", "email", "document", "ip", "wallet", "payment_wallet", "address"]
DEFAULT_FRAUD_SIZES = [20, 35, 50, 80, 100]
HONEST_OVERLAP_RATIO = 0.015  # 1.5 % of honest apps share one attribute


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Generate test data for anti-fraud Memgraph schema.",
    )
    p.add_argument("--honest", type=int, default=10_000,
                   help="honest applications (default: 10000)")
    p.add_argument("--fraud-clusters", type=int, default=5,
                   help="number of fraud clusters (default: 5)")
    p.add_argument("--fraud-sizes", type=str, default=None,
                   help="comma-separated cluster sizes (default: 20,35,50,80,100)")
    p.add_argument("--gray-clusters", type=int, default=2,
                   help="gray-zone clusters (default: 2)")
    p.add_argument("--gray-size", type=int, default=50,
                   help="size of each gray cluster (default: 50)")
    p.add_argument("--output", choices=["cypher"], default="cypher",
                   help="output format (default: cypher)")
    p.add_argument("--output-file", "-o", type=str, default=None,
                   help="write to file (default: stdout)")
    p.add_argument("--batch-size", type=int, default=500,
                   help="UNWIND batch size (default: 500)")
    p.add_argument("--seed", type=int, default=42,
                   help="random seed (default: 42)")
    return p.parse_args()


# ── helpers ───────────────────────────────────────────────────────────────────

def escape_cypher(s: str) -> str:
    """Escape a string for a single-quoted Cypher literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


# ── generator ─────────────────────────────────────────────────────────────────

class AppGenerator:
    """Produces Application dicts with realistic, mostly-unique attributes."""

    def __init__(self, seed: int = 42):
        self.fake = Faker("ru_RU")
        Faker.seed(seed)
        random.seed(seed)
        self._counter = 0
        self._used: dict[str, set] = defaultdict(set)

    # -- id ----------------------------------------------------------------

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter:06d}"

    # -- unique value generators -------------------------------------------

    def _unique(self, attr: str, gen) -> str:
        for _ in range(10_000):
            val = gen()
            if val not in self._used[attr]:
                self._used[attr].add(val)
                return val
        raise RuntimeError(f"Cannot generate unique {attr} after 10 000 attempts")

    def unique_phone(self):
        return self._unique(
            "phone",
            lambda: f"+7{random.randint(900, 999)}{random.randint(1000000, 9999999)}",
        )

    def unique_email(self):
        return self._unique("email", self.fake.email)

    def unique_document(self):
        return self._unique(
            "document",
            lambda: f"{random.randint(1000, 9999)} {random.randint(100000, 999999)}",
        )

    def unique_ip(self):
        return self._unique("ip", self.fake.ipv4)

    def unique_wallet(self):
        return self._unique("wallet", lambda: f"0x{uuid.uuid4().hex[:40]}")

    def unique_payment_wallet(self):
        return self._unique(
            "payment_wallet", lambda: f"T{uuid.uuid4().hex[:33].upper()}"
        )

    def unique_address(self):
        return self._unique(
            "address", lambda: self.fake.address().replace("\n", ", ")
        )

    # -- random scalars ----------------------------------------------------

    def _random_ts(self, days_back: int = 730) -> str:
        dt = datetime.now() - timedelta(seconds=random.randint(0, days_back * 86400))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def _random_amount(self) -> float:
        return round(random.uniform(5_000, 500_000), 2)

    # -- batch generators --------------------------------------------------

    def generate_honest(self, count: int) -> list[dict]:
        """10 000 honest apps: unique attrs, 80/15/5 status split."""
        status_pool = ["approved"] * 80 + ["rejected"] * 15 + ["pending"] * 5
        apps = []
        for _ in range(count):
            apps.append({
                "id": self._next_id("honest"),
                "created_at": self._random_ts(),
                "phone": self.unique_phone(),
                "email": self.unique_email(),
                "document": self.unique_document(),
                "ip": self.unique_ip(),
                "wallet": self.unique_wallet(),
                "payment_wallet": self.unique_payment_wallet(),
                "address": self.unique_address(),
                "amount": self._random_amount(),
                "status": random.choice(status_pool),
            })
        return apps

    @staticmethod
    def inject_overlaps(apps: list[dict], ratio: float = HONEST_OVERLAP_RATIO):
        """Randomly copy one attribute between ~ratio*len pairs (accidental overlaps)."""
        n_pairs = int(len(apps) * ratio / 2)
        indices = range(len(apps))
        for _ in range(n_pairs):
            i, j = random.sample(indices, 2)
            attr = random.choice(LINK_ATTRS)
            apps[j][attr] = apps[i][attr]

    def generate_fraud_cluster(self, cluster_id: int, size: int) -> list[dict]:
        """Fraud ring: 3-5 phones, 3-5 docs, 2-4 IPs, 2-4 wallets shared."""
        phones  = [self.unique_phone()    for _ in range(random.randint(3, 5))]
        docs    = [self.unique_document() for _ in range(random.randint(3, 5))]
        ips     = [self.unique_ip()       for _ in range(random.randint(2, 4))]
        wallets = [self.unique_wallet()   for _ in range(random.randint(2, 4))]

        apps = []
        for _ in range(size):
            apps.append({
                "id": self._next_id("fraud"),
                "created_at": self._random_ts(days_back=365),
                "phone": random.choice(phones),
                "email": self.unique_email(),
                "document": random.choice(docs),
                "ip": random.choice(ips),
                "wallet": random.choice(wallets),
                "payment_wallet": self.unique_payment_wallet(),
                "address": self.unique_address(),
                "amount": self._random_amount(),
                "status": "fraud",
                "cluster_id": cluster_id,
                "fraud_score": round(random.uniform(0.75, 1.0), 4),
            })
        return apps

    def generate_gray_cluster(
        self,
        cluster_id: int,
        size: int,
        honest_phone_pool: list[str],
    ) -> list[dict]:
        """Gray zone: partial reuse inside + 20 % cross-links to honest phones."""
        phones = [self.unique_phone() for _ in range(random.randint(8, 12))]
        emails = [self.unique_email() for _ in range(random.randint(5, 8))]
        status_pool = (
            ["approved"] * 40
            + ["rejected"] * 30
            + ["pending"] * 20
            + ["fraud"] * 10
        )

        apps = []
        for _ in range(size):
            # 20 % chance to borrow a phone from an honest app → cross-link
            phone = random.choice(phones)
            if honest_phone_pool and random.random() < 0.20:
                phone = random.choice(honest_phone_pool)

            email = (
                random.choice(emails) if random.random() < 0.5
                else self.unique_email()
            )

            apps.append({
                "id": self._next_id("gray"),
                "created_at": self._random_ts(days_back=365),
                "phone": phone,
                "email": email,
                "document": self.unique_document(),
                "ip": self.unique_ip(),
                "wallet": self.unique_wallet(),
                "payment_wallet": self.unique_payment_wallet(),
                "address": self.unique_address(),
                "amount": self._random_amount(),
                "status": random.choice(status_pool),
                "cluster_id": cluster_id,
                "fraud_score": round(random.uniform(0.3, 0.7), 4),
            })
        return apps


# ── edge estimation ───────────────────────────────────────────────────────────

def compute_stats(all_apps: list[dict]):
    """Estimate LINKED edges by checking all attribute overlaps.

    Returns (total_edges, weight_distribution, max_weight, edges_by_category).
    """
    # inverted index: (attr_name, attr_value) → {app_id, …}
    inv: dict[tuple, set] = defaultdict(set)
    for app in all_apps:
        for attr in LINK_ATTRS:
            val = app.get(attr)
            if val is not None:
                inv[(attr, val)].add(app["id"])

    # for every pair sharing ≥1 attribute, collect which attributes match
    pair_attrs: dict[tuple, set] = defaultdict(set)
    for (attr, _), ids in inv.items():
        if len(ids) < 2:
            continue
        for a, b in combinations(sorted(ids), 2):
            pair_attrs[(a, b)].add(attr)

    # aggregate
    def _cat(app_id: str) -> str:
        return app_id.split("_", 1)[0]  # honest | fraud | gray

    total = len(pair_attrs)
    weight_dist: dict[int, int] = defaultdict(int)
    by_category: dict[tuple, int] = defaultdict(int)
    max_w = 0

    for (a, b), attrs in pair_attrs.items():
        w = len(attrs)
        weight_dist[w] += 1
        max_w = max(max_w, w)
        key = tuple(sorted([_cat(a), _cat(b)]))
        by_category[key] += 1

    return total, weight_dist, max_w, by_category


# ── cypher output ─────────────────────────────────────────────────────────────

def app_to_map(app: dict) -> str:
    """Render one Application dict as a Cypher map literal."""
    parts: list[str] = []

    for key in (
        "id", "created_at", "phone", "email", "document",
        "ip", "wallet", "payment_wallet", "address", "status",
    ):
        val = app.get(key)
        if val is not None:
            parts.append(f"{key}: '{escape_cypher(val)}'")

    # numeric / optional fields
    parts.append(f"amount: {app['amount']}")
    if app.get("cluster_id") is not None:
        parts.append(f"cluster_id: {app['cluster_id']}")
    if app.get("fraud_score") is not None:
        parts.append(f"fraud_score: {app['fraud_score']}")

    return "{" + ", ".join(parts) + "}"


def write_cypher(apps: list[dict], batch_size: int, out):
    """Write UNWIND-based Cypher INSERT batches."""
    for i in range(0, len(apps), batch_size):
        batch = apps[i : i + batch_size]
        out.write("UNWIND [\n")
        for j, app in enumerate(batch):
            comma = "," if j < len(batch) - 1 else ""
            out.write(f"  {app_to_map(app)}{comma}\n")
        out.write("] AS row\n")
        out.write("CREATE (a:Application)\n")
        out.write("SET a += row,\n")
        out.write("    a.created_at = localDateTime(row.created_at);\n\n")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # resolve fraud cluster sizes
    if args.fraud_sizes:
        fraud_sizes = [int(x) for x in args.fraud_sizes.split(",")]
    else:
        fraud_sizes = DEFAULT_FRAUD_SIZES[: args.fraud_clusters]
        while len(fraud_sizes) < args.fraud_clusters:
            fraud_sizes.append(random.randint(15, 120))

    gray_sizes = [args.gray_size] * args.gray_clusters

    log = lambda msg: print(msg, file=sys.stderr)
    gen = AppGenerator(seed=args.seed)

    # ── 1. honest ─────────────────────────────────────────────────
    log("Generating applications...")

    honest = gen.generate_honest(args.honest)
    gen.inject_overlaps(honest)
    log(f"  honest:       {len(honest):>6}")

    # ── 2. fraud clusters ─────────────────────────────────────────
    fraud: list[dict] = []
    for i, sz in enumerate(fraud_sizes):
        cid = i + 1
        cluster = gen.generate_fraud_cluster(cid, sz)
        n_phones = len({a["phone"] for a in cluster})
        n_docs = len({a["document"] for a in cluster})
        fraud.extend(cluster)
        log(f"  fraud #{cid:>2}:     {sz:>4} apps  "
            f"(phones={n_phones}, docs={n_docs})")
    log(f"  fraud total:  {len(fraud):>6}")

    # ── 3. gray clusters ──────────────────────────────────────────
    honest_phones = random.sample(
        [a["phone"] for a in honest], min(30, len(honest))
    ) if honest else []

    gray: list[dict] = []
    for i, sz in enumerate(gray_sizes):
        cid = len(fraud_sizes) + i + 1
        cluster = gen.generate_gray_cluster(cid, sz, honest_phones)
        gray.extend(cluster)
        log(f"  gray   #{cid:>2}:    {sz:>4} apps")
    log(f"  gray total:   {len(gray):>6}")

    all_apps = honest + fraud + gray
    log(f"  ─────────────────────")
    log(f"  TOTAL:        {len(all_apps):>6}")

    # ── 4. edge statistics ────────────────────────────────────────
    log("\nExpected LINKED edges (computed from attribute overlaps):")
    total_edges, w_dist, max_w, by_cat = compute_stats(all_apps)
    log(f"  total edges:  {total_edges}")
    for w in sorted(w_dist):
        log(f"    weight={w}: {w_dist[w]:>6} edges")
    log(f"  max weight:   {max_w}")
    log("  by category:")
    for cat_key in sorted(by_cat):
        log(f"    {cat_key[0]:>7} ↔ {cat_key[1]:<7}: {by_cat[cat_key]:>6}")

    # ── 5. write output ───────────────────────────────────────────
    out = (
        open(args.output_file, "w", encoding="utf-8")
        if args.output_file
        else sys.stdout
    )
    try:
        out.write(
            f"// ── Test data: anti-fraud Memgraph schema ──\n"
            f"// Honest: {len(honest)}  Fraud: {len(fraud)}  "
            f"Gray: {len(gray)}  Total: {len(all_apps)}\n"
            f"// Expected LINKED edges: {total_edges}\n"
            f"// Seed: {args.seed}  Batch: {args.batch_size}\n\n"
        )

        write_cypher(all_apps, args.batch_size, out)

        out.write(f"// ── Summary ──\n")
        out.write(f"// Total applications created: {len(all_apps)}\n")
        out.write(f"// Expected LINKED edges:      {total_edges}\n")
        for w in sorted(w_dist):
            out.write(f"//   weight={w}: {w_dist[w]} edges\n")
        out.write(f"// Max edge weight:            {max_w}\n")
    finally:
        if args.output_file:
            out.close()
            log(f"\nWritten to {args.output_file}")


if __name__ == "__main__":
    main()
