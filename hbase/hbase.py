import json
import datetime
from collections import defaultdict

import happybase

conn = happybase.Connection("localhost", port=9090)
conn.open()

# Column families for each table (they can't be changed freely later)
TABLES = {
    "user_sessions": {"info": dict(), "views": dict()},
    "product_metrics": {"m": dict()},
}

# Drop and recreate so the families match the design
existing = conn.tables()
for name, families in TABLES.items():
    if name.encode() in existing:
        conn.delete_table(name, disable=True)
    conn.create_table(name, families)

print(conn.tables())


user_session_schema = {
    'cf': dict
}

product_metrics_schema = {
    'cf': dict
}


