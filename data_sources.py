# data_sources.py
# Shared Spark session + loaders for the three storage layers:
#   - curated Parquet (written by dataframe.py from the JSON files)
#   - MongoDB  (users, products, transactions - see mongo_model.py)
#   - HBase    (10% session sample in user_sessions, incl. page views - see hbase/hbase.py)
#
# Each database loader falls back to the curated Parquet copy of the same data when the
# database is not reachable, so the analytics scripts always run; the source actually used
# is printed.
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, TimestampType, BooleanType)

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


# MongoDB documents have an ObjectId as _id. Spark has no ObjectId type, so it is kept as
# its 24-character hex string in a "mongo_id" column. Joins use the natural ids
# (user_id, transaction_id, ...), which are the same in MongoDB, HBase and the Parquet files.
USERS_SCHEMA = StructType([
    StructField("mongo_id", StringType()), StructField("user_id", StringType()),
    StructField("city", StringType()), StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("registration_date", TimestampType()), StructField("last_active", TimestampType()),
])


def mongo_users(spark):
    try:
        db = _mongo_db()
        projection = {"user_id": 1, "geo_data": 1, "registration_date": 1, "last_active": 1}
        rows = [(str(d["_id"]), d["user_id"], d["geo_data"].get("city"), d["geo_data"].get("state"),
                 d["geo_data"].get("country"), d["registration_date"], d.get("last_active"))
                for d in db.users.find({}, projection)]
        print(f"  users            <- MongoDB ({len(rows):,} documents)")
        return spark.createDataFrame(rows, USERS_SCHEMA)
    except Exception as e:
        print(f"  users            <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/users")
                .withColumn("mongo_id", F.lit(None).cast("string"))
                .select(*[f.name for f in USERS_SCHEMA]))


TXN_SAMPLE_SCHEMA = StructType([
    StructField("mongo_id", StringType()),
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
            {"$project": {"transaction_id": 1, "user_id": 1, "session_id": 1, "timestamp": 1,
                          "total": 1, "item_count": {"$size": "$items"},
                          "payment_method": 1, "status": 1}},
        ]
        rows = [(str(d["_id"]), d["transaction_id"], d["user_id"], d.get("session_id"),
                 d["timestamp"], float(d["total"]), d["item_count"], d["payment_method"], d["status"])
                for d in db.transactions.aggregate(pipeline, allowDiskUse=True)]
        print(f"  transactions     <- MongoDB ($sample of {len(rows):,})")
        return spark.createDataFrame(rows, TXN_SAMPLE_SCHEMA)
    except Exception as e:
        print(f"  transactions     <- curated Parquet sample (MongoDB unavailable: {type(e).__name__})")
        txns = spark.read.parquet(f"{CURATED_PATH}/transactions")
        return (txns.sample(fraction=size / 500_000, seed=HBASE_SAMPLE_SEED)
                .withColumn("mongo_id", F.lit(None).cast("string"))
                .select(*[f.name for f in TXN_SAMPLE_SCHEMA]))


BASKET_SCHEMA = StructType([
    StructField("mongo_id", StringType()), StructField("transaction_id", StringType()),
    StructField("user_id", StringType()), StructField("product_id", StringType()),
])


def mongo_order_baskets(spark):
    """One row per (order, product) from MongoDB transactions. Only the ids are
    projected, so MongoDB sends ~1M small rows instead of whole order documents."""
    try:
        db = _mongo_db()
        cursor = db.transactions.find({}, {"transaction_id": 1, "user_id": 1, "items.product_id": 1},
                                      batch_size=10_000)
        rows = [(str(d["_id"]), d["transaction_id"], d["user_id"], item["product_id"])
                for d in cursor for item in d["items"]]
        print(f"  order baskets    <- MongoDB transactions ({len(rows):,} order lines)")
        return spark.createDataFrame(rows, BASKET_SCHEMA)
    except Exception as e:
        print(f"  order baskets    <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/transaction_items")
                .withColumn("mongo_id", F.lit(None).cast("string"))
                .select(*[f.name for f in BASKET_SCHEMA]))


PRODUCTS_SCHEMA = StructType([
    StructField("mongo_id", StringType()), StructField("product_id", StringType()),
    StructField("name", StringType()), StructField("category_id", StringType()),
    StructField("base_price", DoubleType()), StructField("is_active", BooleanType()),
])


def mongo_products(spark):
    """Product catalog from MongoDB (names, category, price, active flag)."""
    try:
        db = _mongo_db()
        rows = [(str(d["_id"]), d["product_id"], d["name"], d["category_id"],
                 float(d["base_price"]), bool(d["is_active"]))
                for d in db.products.find({}, {"product_id": 1, "name": 1, "category_id": 1,
                                               "base_price": 1, "is_active": 1})]
        print(f"  products         <- MongoDB ({len(rows):,} documents)")
        return spark.createDataFrame(rows, PRODUCTS_SCHEMA)
    except Exception as e:
        print(f"  products         <- curated Parquet (MongoDB unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/products")
                .withColumn("mongo_id", F.lit(None).cast("string"))
                .select(*[f.name for f in PRODUCTS_SCHEMA]))


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


VIEWS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("session_id", StringType()),
    StructField("product_id", StringType()),
])


def hbase_product_views(spark):
    """Product detail page views from HBase user_sessions (views family: one JSON
    column per page view). One row per (session, viewed product)."""
    try:
        import happybase
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60_000)
        table = conn.table("user_sessions")
        rows = []
        for key, d in table.scan(columns=[b"views"], batch_size=2000):
            user_id, _, session_id = key.decode().split("#")
            for value in d.values():
                view = json.loads(value)
                if view.get("page_type") == "product_detail" and view.get("product_id"):
                    rows.append((user_id, session_id, view["product_id"]))
        conn.close()
        if not rows:
            raise RuntimeError("user_sessions has no views")
        print(f"  product views    <- HBase user_sessions views family ({len(rows):,} views)")
        return spark.createDataFrame(rows, VIEWS_SCHEMA)
    except Exception as e:
        print(f"  product views    <- curated Parquet {HBASE_SAMPLE_FRACTION:.0%} sample "
              f"(HBase unavailable: {type(e).__name__})")
        return (spark.read.parquet(f"{CURATED_PATH}/sessions")
                .sample(fraction=HBASE_SAMPLE_FRACTION, seed=HBASE_SAMPLE_SEED)
                .select("user_id", "session_id", F.explode("viewed_products").alias("product_id")))
