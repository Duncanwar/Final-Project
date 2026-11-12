# data_sources.py
# Shared Spark session + loaders for the three storage layers:
#   - curated Parquet (written by dataframe.py from the JSON files)
#   - MongoDB  (users, products, categories, transactions - see mongo_model.py)
#   - HBase    (10% session sample in user_sessions - see hbase/hbase.py)
#
# Each database loader falls back to the curated Parquet copy of the same data when the
# database is not reachable, so the analytics scripts always run; the source actually used
# is printed.
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, TimestampType)

CURATED_PATH = "./parquet/curated"
ANALYTICS_PATH = "./parquet/analytics"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "ecommerce"
HBASE_HOST, HBASE_PORT = "localhost", 9090

HBASE_SAMPLE_FRACTION = 0.10   # share of sessions loaded into HBase (hbase/hbase.py)
HBASE_SAMPLE_SEED = 42


def get_spark(app_name, memory="3g"):
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[3]")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.driver.memory", memory)
             .config("spark.network.timeout", "600s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.sql.shuffle.partitions", "24")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_curated(spark, names):
    """Register curated Parquet tables as Spark SQL temp views."""
    for name in names:
        spark.read.parquet(f"{CURATED_PATH}/{name}").createOrReplaceTempView(name)


def save(df, name):
    """Write an analytics result (small, one file) and return it re-read from disk."""
    path = f"{ANALYTICS_PATH}/{name}"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    return df.sparkSession.read.parquet(path)


# --- MongoDB ---
def _mongo_db():
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    client.admin.command("ping")
    return client[MONGO_DB]


USERS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("city", StringType()),
    StructField("state", StringType()), StructField("country", StringType()),
    StructField("registration_date", TimestampType()), StructField("last_active", TimestampType()),
])


def mongo_users(spark):
    try:
        db = _mongo_db()
        rows = [(d["_id"], d["geo_data"].get("city"), d["geo_data"].get("state"),
                 d["geo_data"].get("country"), d["registration_date"], d.get("last_active"))
                for d in db.users.find({}, {"geo_data": 1, "registration_date": 1, "last_active": 1})]
        print(f"  users            <- MongoDB ({len(rows):,} documents)")
        return spark.createDataFrame(rows, USERS_SCHEMA)
    except Exception as e:
        print(f"  users            <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return spark.read.parquet(f"{CURATED_PATH}/users")


TXN_SAMPLE_SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("user_id", StringType()),
    StructField("session_id", StringType()), StructField("timestamp", TimestampType()),
    StructField("total", DoubleType()), StructField("item_count", IntegerType()),
    StructField("payment_method", StringType()), StructField("status", StringType()),
])


def mongo_transactions_sample(spark, size=50_000):
    """Random sample of orders, drawn inside MongoDB with $sample."""
    try:
        db = _mongo_db()
        pipeline = [
            {"$sample": {"size": size}},
            {"$project": {"user_id": 1, "session_id": 1, "timestamp": 1, "total": 1,
                          "item_count": {"$size": "$items"}, "payment_method": 1, "status": 1}},
        ]
        rows = [(d["_id"], d["user_id"], d.get("session_id"), d["timestamp"], float(d["total"]),
                 d["item_count"], d["payment_method"], d["status"])
                for d in db.transactions.aggregate(pipeline, allowDiskUse=True)]
        print(f"  transactions     <- MongoDB ($sample of {len(rows):,})")
        return spark.createDataFrame(rows, TXN_SAMPLE_SCHEMA)
    except Exception as e:
        print(f"  transactions     <- curated Parquet sample (MongoDB unavailable: {type(e).__name__})")
        txns = spark.read.parquet(f"{CURATED_PATH}/transactions")
        return (txns.sample(fraction=size / 500_000, seed=HBASE_SAMPLE_SEED)
                .select(*[f.name for f in TXN_SAMPLE_SCHEMA]))


CUSTOMER_ORDERS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("orders", IntegerType()),
    StructField("revenue", DoubleType()), StructField("discounts", DoubleType()),
    StructField("items", IntegerType()),
    StructField("first_order", TimestampType()), StructField("last_order", TimestampType()),
])


def mongo_customer_orders(spark):
    """Order history per customer, aggregated inside MongoDB (push-down) so only
    one row per user travels to Spark instead of 500,000 orders."""
    try:
        db = _mongo_db()
        pipeline = [{"$group": {
            "_id": "$user_id",
            "orders": {"$sum": 1},
            "revenue": {"$sum": "$total"},
            "discounts": {"$sum": "$discount"},
            "items": {"$sum": {"$size": "$items"}},
            "first_order": {"$min": "$timestamp"},
            "last_order": {"$max": "$timestamp"},
        }}]
        rows = [(d["_id"], d["orders"], float(d["revenue"]), float(d["discounts"]), d["items"],
                 d["first_order"], d["last_order"])
                for d in db.transactions.aggregate(pipeline, allowDiskUse=True)]
        print(f"  customer orders  <- MongoDB aggregation ({len(rows):,} customers)")
        return spark.createDataFrame(rows, CUSTOMER_ORDERS_SCHEMA)
    except Exception as e:
        print(f"  customer orders  <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/transactions")
                .groupBy("user_id")
                .agg(F.count("*").cast("int").alias("orders"), F.sum("total").alias("revenue"),
                     F.sum("discount").alias("discounts"), F.sum("item_count").cast("int").alias("items"),
                     F.min("timestamp").alias("first_order"), F.max("timestamp").alias("last_order")))


def mongo_avg_profit_margin():
    """Average subcategory profit margin from the categories collection."""
    try:
        db = _mongo_db()
        result = list(db.categories.aggregate([
            {"$unwind": "$subcategories"},
            {"$group": {"_id": None, "margin": {"$avg": "$subcategories.profit_margin"}}}]))
        print(f"  profit margin    <- MongoDB categories")
        return result[0]["margin"]
    except Exception as e:
        import pandas as pd
        print(f"  profit margin    <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return float(pd.read_parquet(f"{CURATED_PATH}/subcategories")["profit_margin"].mean())


# --- HBase ---
SESSIONS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("session_id", StringType()),
    StructField("start_time", TimestampType()), StructField("duration_seconds", IntegerType()),
    StructField("device_type", StringType()), StructField("referrer", StringType()),
    StructField("country", StringType()), StructField("conversion_status", StringType()),
])


def hbase_sessions(spark):
    """Session attributes from HBase user_sessions (info family only; the large views
    family is not read). Row key: user_id#reverse_ts#session_id."""
    try:
        import happybase
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10_000)
        table = conn.table("user_sessions")
        rows = []
        for key, d in table.scan(columns=[b"info"], batch_size=5000):
            user_id, _, session_id = key.decode().split("#")
            rows.append((user_id, session_id,
                         datetime.datetime.fromisoformat(d[b"info:start_time"].decode()),
                         int(d[b"info:duration"]), d[b"info:device"].decode(),
                         d[b"info:referrer"].decode(), d[b"info:country"].decode(),
                         d[b"info:status"].decode()))
        conn.close()
        if not rows:
            raise RuntimeError("user_sessions is empty")
        print(f"  sessions         <- HBase user_sessions ({len(rows):,} rows)")
        return spark.createDataFrame(rows, SESSIONS_SCHEMA)
    except Exception as e:
        print(f"  sessions         <- curated Parquet {HBASE_SAMPLE_FRACTION:.0%} sample "
              f"(HBase unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/sessions")
                .sample(fraction=HBASE_SAMPLE_FRACTION, seed=HBASE_SAMPLE_SEED)
                .select(*[f.name for f in SESSIONS_SCHEMA]))
