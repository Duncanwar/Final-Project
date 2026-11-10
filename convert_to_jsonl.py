# Convert JSON array files to JSON Lines (one record per line) so Spark can read
# them without multiLine.
import glob
import json
import os

os.makedirs("jsonl", exist_ok=True)

for path in sorted(glob.glob("*.json")):
    out_path = os.path.join("jsonl", path.replace(".json", ".jsonl"))
    if os.path.exists(out_path):
        continue                                  # already converted (safe re-runs)

    with open(path) as f:
        records = json.load(f)                    # one file at a time, ~1 GB RAM peak
    with open(out_path, "w") as out:
        for record in records:
            out.write(json.dumps(record) + "\n")
    print(f"{path} -> {out_path}: {len(records):,} records")
