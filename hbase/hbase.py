# hbase.py
# Loads a representative random sample of the session data into HBase.
#
# Tables:
#   user_sessions    row key: user_id#reverse_ts#session_id  (a user's sessions, newest first)
#                    families: info (session attributes), views (one column per page view, JSON)
#   product_metrics  row key: product_id#YYYYMMDD            (per product, per day)
#                    family:   m (views, view_seconds, cart_adds, purchased_qty)
#
# Sampling: the same fraction is drawn at random from every sessions_*.json file
# (fixed seed), so the sample covers all users, products and the full 90-day period.
#
# Usage:
#   cd hbase && docker compose up -d
#   python hbase.py
import os
import glob
import json
import random
import datetime
from collections import Counter, defaultdict

import happybase

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SAMPLE_FRACTION = 0.10  # 10% of 2,000,000 sessions = ~200,000
SEED = 42
BATCH_SIZE = 1000

conn = happybase.Connection("localhost", port=9090)
conn.open()

# Column families for each table (they can't be changed freely later)
TABLES = {
    "user_sessions": {"info": dict(), "views": dict()},
    "product_metrics": {"m": dict()},
}

MAX_TS = 9999999999  # used to reverse timestamps so newest sorts first


def create_tables():
    # Drop and recreate so the families match the design
    existing = conn.tables()
    for name, families in TABLES.items():
        if name.encode() in existing:
            conn.delete_table(name, disable=True)
        conn.create_table(name, families)
    print(f"Tables ready: {conn.tables()}")


def session_row_key(session):
    start = datetime.datetime.fromisoformat(session["start_time"])
    reverse_ts = MAX_TS - int(start.timestamp())
    return f"{session['user_id']}#{reverse_ts:010d}#{session['session_id']}".encode()


def product_row_key(product_id, timestamp):
    day = timestamp[:10].replace("-", "")  # "2026-09-15T..." -> "20260915"
    return f"{product_id}#{day}".encode()


def session_columns(session):
    device = session["device_profile"]
    cols = {
        b"info:start_time": session["start_time"],
        b"info:end_time": session["end_time"],
        b"info:duration": str(session["duration_seconds"]),
        b"info:device": device["type"],
        b"info:os": device["os"],
        b"info:browser": device["browser"],
        b"info:referrer": session["referrer"],
        b"info:status": session["conversion_status"],
        b"info:country": session["geo_data"]["country"],
    }
    # One column per page view: views:000, views:001, ...
    for i, view in enumerate(session["page_views"]):
        cols[f"views:{i:03d}".encode()] = json.dumps(view)
    return {k: v.encode() for k, v in cols.items()}


def add_product_metrics(metrics, session):
    # Product views and viewing time, per product per day
    for view in session["page_views"]:
        if view["page_type"] == "product_detail" and view["product_id"]:
            key = product_row_key(view["product_id"], view["timestamp"])
            metrics[key]["views"] += 1
            metrics[key]["view_seconds"] += view["view_duration"]

    # Cart adds, and quantities actually bought in converted sessions
    for product_id, item in session["cart_contents"].items():
        key = product_row_key(product_id, session["start_time"])
        metrics[key]["cart_adds"] += item["quantity"]
        if session["conversion_status"] == "converted":
            metrics[key]["purchased_qty"] += item["quantity"]


def load_sample():
    rng = random.Random(SEED)
    sessions_table = conn.table("user_sessions")
    metrics = defaultdict(lambda: defaultdict(int))  # row key -> {column: count}
    stats = Counter()  # sample profile, used to check it is representative
    total_seen = 0

    files = sorted(glob.glob(os.path.join(DATA_DIR, "sessions_*.json")),
                   key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0]))

    # One file at a time to keep memory usage down
    for path in files:
        with open(path) as f:
            sessions = json.load(f)
        total_seen += len(sessions)
        sample = rng.sample(sessions, int(len(sessions) * SAMPLE_FRACTION))

        with sessions_table.batch(batch_size=BATCH_SIZE) as batch:
            for s in sample:
                batch.put(session_row_key(s), session_columns(s))
                add_product_metrics(metrics, s)
                stats["sessions"] += 1
                stats[f"status:{s['conversion_status']}"] += 1
                stats[f"device:{s['device_profile']['type']}"] += 1
                stats["page_views"] += len(s["page_views"])

        print(f"{os.path.basename(path)}: {len(sample):,} of {len(sessions):,} sessions loaded")
        del sessions, sample

    # Product metrics are written once, after counting across the whole sample
    metrics_table = conn.table("product_metrics")
    with metrics_table.batch(batch_size=BATCH_SIZE) as batch:
        for key, counts in metrics.items():
            batch.put(key, {f"m:{col}".encode(): str(n).encode() for col, n in counts.items()})

    print(f"\nLoaded {stats['sessions']:,} of {total_seen:,} sessions "
          f"({stats['sessions'] / total_seen:.1%}) and {len(metrics):,} product-day rows")
    return stats


def print_profile(stats):
    # Shares in the sample should match the full dataset if it is representative
    n = stats["sessions"]
    print("\nSample profile:")
    for label in sorted(k for k in stats if ":" in k):
        print(f"  {label:<22} {stats[label] / n:6.1%}")
    print(f"  {'page views / session':<22} {stats['page_views'] / n:6.2f}")


def example_queries():
    sessions_table = conn.table("user_sessions")
    metrics_table = conn.table("product_metrics")

    user_id = "user_000123"
    print(f"\nLatest sessions for {user_id}:")
    for key, data in sessions_table.scan(row_prefix=f"{user_id}#".encode(), limit=5):
        print(f"  {data[b'info:start_time'].decode()}  {data[b'info:status'].decode():<9}  {key.decode()}")

    product_id = "prod_00042"
    print(f"\n{product_id} daily metrics, 2026-09-01 to 2026-09-30:")
    for key, data in metrics_table.scan(row_start=f"{product_id}#20260901".encode(),
                                        row_stop=f"{product_id}#20261001".encode()):
        print(f"  {key.decode()}  {({k.decode(): v.decode() for k, v in data.items()})}")


if __name__ == "__main__":
    create_tables()
    stats = load_sample()
    print_profile(stats)
    example_queries()
    conn.close()
