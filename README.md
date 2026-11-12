# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics final project. It analyses 90 days of synthetic e-commerce
data (2.5 million records) with three technologies, each used for what it does best:

| Technology | Role in this project |
| --- | --- |
| **MongoDB** (document store) | Holds all data as documents: products, users, categories, transactions (with embedded line items) and sessions (with embedded page views). Runs the aggregation pipelines. |
| **HBase** (wide-column store, in Docker) | Holds a 10% sample of sessions, keyed so a user's sessions can be read newest first, plus daily metrics per product. |
| **Apache Spark** (PySpark) | Cleans the data, computes product recommendations, runs Spark SQL analytics, and joins MongoDB + HBase data for the customer lifetime value (CLV) analysis. |

The technical report (`Final_Report.docx` / PDF) explains the design and the results.

## How the pipeline fits together

```
dataset_generator.py ──> raw JSON files (users, products, categories, transactions, sessions_0..19)
        │
        ├── mongo_model.py ──────> MongoDB  (all data, validated, indexed)
        │                             └── mongo_aggregations.py  (aggregation pipelines)
        ├── hbase/hbase.py ──────> HBase    (10% session sample + product metrics)
        └── convert_to_jsonl.py ─> jsonl/   (one record per line, fast for Spark)
                                     │
                                     └── dataframe.py ──> parquet/curated/  (cleaned tables)
                                                          parquet/recommendations/
                                            │
              MongoDB + HBase + curated ──> spark_sql_analytics.py ──> parquet/analytics/
                                            integrated_clv.py ─────> parquet/analytics/
                                                                         │
                                                visualizations.py ──> figures/*.png
```

## Files

| File | What it does | Assignment part |
| --- | --- | --- |
| `dataset_generator.py` | Generates the synthetic dataset (provided by the course) | Dataset |
| `mongo_model.py` | Creates MongoDB collections with schema validators and indexes, then loads all JSON files | Part 1 |
| `mongo_aggregations.py` | Two aggregation pipelines: most viewed products, and customers by purchase frequency | Part 1 |
| `hbase/Dockerfile`, `hbase/docker-compose.yml`, `hbase/hbase-site.xml` | Builds and runs HBase 2.5.10 in Docker (standalone, Thrift on port 9090) | Part 1 |
| `hbase/hbase.py` | Creates the HBase tables, loads the session sample and product metrics, runs example queries | Part 1 |
| `convert_to_jsonl.py` | Converts the JSON arrays to JSON Lines so Spark can split and read them quickly | Part 2 |
| `dataframe.py` | Spark batch job: cleans and normalizes all data, computes "bought X also bought Y" recommendations | Part 2 |
| `data_sources.py` | Shared helpers: Spark session, and loaders that read MongoDB and HBase into Spark DataFrames | Parts 2–3 |
| `spark_sql_analytics.py` | Spark SQL queries on the cleaned data (Part A) and on data loaded from MongoDB/HBase (Part B) | Part 2 |
| `integrated_clv.py` | Integrated query: customer lifetime value from MongoDB users/orders + HBase sessions | Part 3 |
| `visualizations.py` | Draws the four report charts into `figures/` | Part 4 |
| `figures/` | The four charts (PNG) | Part 4 |

Generated data (`*.json`, `jsonl/`, `parquet/`) is git-ignored because it is several GB.

## Requirements

- Python 3.11+ (tested with 3.13) and Java 17 (for Spark)
- MongoDB running on `localhost:27017`
- Docker Desktop (for HBase), with at least 4 GB of memory
- About 8 GB RAM and 10 GB free disk

Python packages (tested versions):

```bash
python -m venv .venv
source .venv/bin/activate
pip install faker pandas pymongo happybase pyspark matplotlib pyarrow
# tested: pyspark 4.3.0, pymongo 4.19.0, happybase 1.3.0, pandas 3.1.0,
#         matplotlib 3.11.2, pyarrow 26.0.1, faker 40.44.0
```

## How to run (in this order)

All commands run from the project folder with the virtual environment active.

| Step | Command | Approx. time (8 GB laptop) | Result |
| --- | --- | --- | --- |
| 1. Generate the data | `python dataset_generator.py` | ~10 min | `users.json`, `products.json`, `categories.json`, `transactions.json`, `sessions_0.json` … `sessions_19.json` |
| 2. Load MongoDB | `python mongo_model.py` | ~10 min | Database `ecommerce`: 25 categories, 5,000 products, 10,000 users, 500,000 transactions, 2,000,000 sessions |
| 3. MongoDB pipelines | `python mongo_aggregations.py` | several minutes | Top 10 most viewed products, then top 10 customers by completed orders |
| 4. Start HBase | `cd hbase && docker compose up -d --build && cd ..` | ~20 min first build, then seconds | HBase UI at http://localhost:16010 |
| 5. Load HBase | `python hbase/hbase.py` | ~10 min | 200,000 sessions in `user_sessions`, 292,679 rows in `product_metrics`, example queries printed |
| 6. Convert for Spark | `python convert_to_jsonl.py` | ~5 min | `jsonl/*.jsonl` |
| 7. Clean + recommend | `python dataframe.py` | ~5 min first run | `parquet/curated/*`, `parquet/recommendations/also_bought` (18,990 pairs) |
| 8. Spark SQL | `python spark_sql_analytics.py` | ~1 min | Query results printed and saved to `parquet/analytics/` |
| 9. CLV | `python integrated_clv.py` | ~1 min | CLV per customer and per segment, saved to `parquet/analytics/` |
| 10. Charts | `python visualizations.py` | seconds | `figures/1_sales_over_time.png` … `figures/4_customer_segments.png` |

Steps 8 and 9 read live from MongoDB and HBase. If either store is not running, they
print a message and use the cleaned Parquet copy instead, so they still finish.

## Sample queries

**MongoDB** (`mongosh ecommerce`):

```js
// A customer's 5 most recent orders (uses the user_id + timestamp index)
db.transactions.find({ user_id: "user_000123" }).sort({ timestamp: -1 }).limit(5)

// Revenue by order status
db.transactions.aggregate([{ $group: { _id: "$status", revenue: { $sum: "$total" } } }])
```

**HBase** (`docker exec -it hbase hbase shell`):

```ruby
# Tables (hbase/hbase.py creates them the same way)
create 'user_sessions', 'info', 'views'
create 'product_metrics', 'm'

# A user's sessions, newest first (row key = user_id#reverse_timestamp#session_id)
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000123#', COLUMNS => ['info:start_time', 'info:status'], LIMIT => 5}

# One product's daily metrics for September 2026 (row key = product_id#YYYYMMDD)
scan 'product_metrics', {STARTROW => 'prod_00042#20260901', STOPROW => 'prod_00042#20261001'}
```

## Key results

- Sales are flat at about **$5.2M per day** over the 90 days.
- Product views and sales are strongly linked (**r = 0.91**), yet **none** of the 10 most viewed products is among the 10 best sellers.
- Session conversion is **~20%** on every device and traffic source.
- Platinum customers are worth **1.6×** Bronze customers ($171,639 vs $110,475 predicted CLV). Value is driven by **order frequency (r = 0.82)**, not browsing (r = 0.06).

## Troubleshooting

| Problem | Fix |
| --- | --- |
| Spark is very slow or loses heartbeats | Close other apps; stop HBase/MongoDB while running `dataframe.py`. The scripts use `local[3]` and 3–4 GB driver memory for an 8 GB machine. |
| `dataframe.py` re-run is slow | It is not: the sessions Parquet is only rebuilt when `parquet/sessions/_SUCCESS` is missing. Delete that folder to force a rebuild. |
| HBase connection refused | Check `docker ps` shows `hbase` up, then wait ~30 s after start for Thrift (port 9090). |
| MongoDB `BulkWriteError` on reload | `mongo_model.py` drops and recreates the collections each run, so simply run it again. |
