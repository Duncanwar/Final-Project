# dataframe.py
# Spark batch job: clean + normalize the raw e-commerce dataset, then compute
# "customers who bought X also bought Y" recommendation indicators.
#
# Input:
#   ./jsonl/*.jsonl          users, products, categories, transactions, sessions (JSON Lines)
#   ./parquet/sessions       sessions converted once from JSON Lines to Parquet
# Output (Parquet, overwritten on every run):
#   ./parquet/curated/{users,products,categories,subcategories,transactions,
#                      transaction_items,sessions}
#   ./parquet/recommendations/also_bought
#
# Usage:
#   python dataframe.py
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_PATH = "./jsonl"
SESSIONS_RAW_PATH = "./parquet/sessions"
CURATED_PATH = "./parquet/curated"
RECOMMENDATIONS_PATH = "./parquet/recommendations/also_bought"

MIN_PAIR_COUNT = 2      # a pair must appear in at least this many orders
TOP_K = 5               # recommendations kept per product


def report(name, raw_count, df):
    clean_count = df.count()
    print(f"{name:<18} raw={raw_count:>10,}  clean={clean_count:>10,}  removed={raw_count - clean_count:,}")


def clean_str(col, case=None, default=None):
    """Trim, turn empty strings into null, optionally change case and fill a default."""
    c = F.trim(F.col(col))
    c = F.when(c == "", None).otherwise(c)
    if case == "lower":
        c = F.lower(c)
    elif case == "upper":
        c = F.upper(c)
    elif case == "title":
        c = F.initcap(c)
    return F.coalesce(c, F.lit(default)) if default is not None else c


# --- Cleaning ---
def clean_users(raw):
    return (raw
            .filter(F.col("user_id").isNotNull())
            .dropDuplicates(["user_id"])
            .select(
                F.col("user_id"),
                clean_str("geo_data.city", "title", "Unknown").alias("city"),
                clean_str("geo_data.state", "upper", "UNKNOWN").alias("state"),
                clean_str("geo_data.country", "upper", "UNKNOWN").alias("country"),
                F.to_timestamp("registration_date").alias("registration_date"),
                F.to_timestamp("last_active").alias("last_active"))
            .filter(F.col("registration_date").isNotNull())
            # a user cannot be active before registering
            .withColumn("last_active", F.greatest("last_active", "registration_date")))


def clean_products(raw):
    # price_history as (date, price) so sorting the array orders it by date
    history = F.transform("price_history", lambda h: F.struct(
        F.to_timestamp(h["date"]).alias("date"),
        h["price"].cast("double").alias("price")))
    return (raw
            .filter(F.col("product_id").isNotNull())
            .dropDuplicates(["product_id"])
            .select(
                "product_id",
                clean_str("name", default="Unnamed product").alias("name"),
                clean_str("category_id", "lower").alias("category_id"),
                F.col("base_price").cast("double").alias("base_price"),
                F.greatest(F.coalesce(F.col("current_stock").cast("int"), F.lit(0)), F.lit(0)).alias("current_stock"),
                F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
                F.to_timestamp("creation_date").alias("creation_date"),
                F.array_sort(F.coalesce(history, F.array())).alias("price_history"))
            .filter(F.col("base_price") > 0)
            .withColumn("price_changes", F.greatest(F.size("price_history") - 1, F.lit(0)))
            .withColumn("first_price", F.coalesce(F.element_at("price_history", 1)["price"], F.col("base_price"))))


def clean_categories(raw):
    categories = (raw
                  .filter(F.col("category_id").isNotNull())
                  .dropDuplicates(["category_id"]))
    # Normalize the embedded array into its own table (one row per subcategory)
    subcategories = (categories
                     .select("category_id", F.explode("subcategories").alias("s"))
                     .select(
                         "category_id",
                         clean_str("s.subcategory_id", "lower").alias("subcategory_id"),
                         clean_str("s.name", default="Unnamed").alias("name"),
                         F.col("s.profit_margin").cast("double").alias("profit_margin"))
                     .filter(F.col("profit_margin").between(0, 1))
                     .dropDuplicates(["subcategory_id"]))
    categories = categories.select(
        clean_str("category_id", "lower").alias("category_id"),
        clean_str("name", default="Unnamed").alias("name"),
        F.size("subcategories").alias("subcategory_count"))
    return categories, subcategories


def clean_transactions(raw, products):
    txns = (raw
            .filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull())
            .dropDuplicates(["transaction_id"])
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .filter(F.col("timestamp").isNotNull() & (F.size("items") > 0)))

    # One row per order line; keep only valid lines that reference a known product
    items = (txns
             .select("transaction_id", "user_id", "timestamp", F.explode("items").alias("i"))
             .select(
                 "transaction_id", "user_id", "timestamp",
                 F.col("i.product_id").alias("product_id"),
                 F.col("i.quantity").cast("int").alias("quantity"),
                 F.col("i.unit_price").cast("double").alias("unit_price"))
             .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
             .withColumn("subtotal", F.round(F.col("quantity") * F.col("unit_price"), 2))
             .join(products.select("product_id"), "product_id", "left_semi"))

    header = txns.select(
        "transaction_id",
        clean_str("session_id").alias("session_id"),
        "user_id",
        "timestamp",
        F.to_date("timestamp").alias("date"),
        F.size("items").alias("item_count"),
        F.col("subtotal").cast("double").alias("subtotal"),
        F.coalesce(F.col("discount").cast("double"), F.lit(0.0)).alias("discount"),
        F.col("total").cast("double").alias("total"),
        clean_str("payment_method", "lower", "unknown").alias("payment_method"),
        clean_str("status", "lower", "unknown").alias("status"))
    header = (header
              .withColumn("has_session", F.col("session_id").isNotNull())
              # flag orders whose stored total does not match subtotal - discount
              .withColumn("total_mismatch",
                          F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.01))
    return header, items


def clean_sessions(raw):
    # cart_contents is keyed by product id, so Spark infers one struct field per product;
    # it is left out here (viewed_products / page_views carry the product information).
    return (raw
            .filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
            .dropDuplicates(["session_id"])
            .select(
                "session_id",
                "user_id",
                F.to_timestamp("start_time").alias("start_time"),
                F.to_timestamp("end_time").alias("end_time"),
                F.col("duration_seconds").cast("int").alias("duration_seconds"),
                clean_str("device_profile.type", "lower", "unknown").alias("device_type"),
                clean_str("device_profile.os", default="Unknown").alias("os"),
                clean_str("device_profile.browser", default="Unknown").alias("browser"),
                clean_str("geo_data.country", "upper", "UNKNOWN").alias("country"),
                clean_str("referrer", "lower", "direct").alias("referrer"),
                clean_str("conversion_status", "lower", "browsed").alias("conversion_status"),
                F.size("page_views").alias("page_view_count"),
                F.array_distinct(F.filter("viewed_products", lambda p: p.isNotNull())).alias("viewed_products"))
            .filter(F.col("start_time").isNotNull() & (F.col("end_time") >= F.col("start_time")))
            # recompute duration where it is missing or inconsistent with the timestamps
            .withColumn("duration_seconds", F.coalesce(
                "duration_seconds",
                (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")).cast("int")))
            .withColumn("date", F.to_date("start_time")))


# --- Recommendations: customers who bought X also bought Y ---
def also_bought(items, products):
    baskets = items.select("transaction_id", "product_id").distinct()
    n_orders = baskets.select("transaction_id").distinct().count()

    product_orders = baskets.groupBy("product_id").agg(F.count("*").alias("orders"))

    # Every unordered pair of different products in the same order
    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col("a.transaction_id") == F.col("b.transaction_id"))
                    & (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("p1"), F.col("b.product_id").alias("p2"))
             .agg(F.count("*").alias("pair_orders"))
             .filter(F.col("pair_orders") >= MIN_PAIR_COUNT))

    # Both directions: X -> Y and Y -> X
    rules = (pairs.select(F.col("p1").alias("product_id"), F.col("p2").alias("also_bought_id"), "pair_orders")
             .unionByName(pairs.select(F.col("p2").alias("product_id"), F.col("p1").alias("also_bought_id"), "pair_orders")))

    x = product_orders.select(F.col("product_id"), F.col("orders").alias("x_orders"))
    y = product_orders.select(F.col("product_id").alias("also_bought_id"), F.col("orders").alias("y_orders"))
    rules = (rules.join(x, "product_id").join(y, "also_bought_id")
             # support    = share of all orders containing both X and Y
             # confidence = P(Y in order | X in order)
             # lift       = confidence / P(Y); > 1 means bought together more than by chance
             .withColumn("support", F.col("pair_orders") / F.lit(n_orders))
             .withColumn("confidence", F.col("pair_orders") / F.col("x_orders"))
             .withColumn("lift", F.col("confidence") / (F.col("y_orders") / F.lit(n_orders))))

    rank = Window.partitionBy("product_id").orderBy(F.desc("confidence"), F.desc("lift"), "also_bought_id")
    names = products.select("product_id", "name")
    return (rules
            .withColumn("rank", F.row_number().over(rank))
            .filter(F.col("rank") <= TOP_K)
            .join(names, "product_id")
            .join(names.select(F.col("product_id").alias("also_bought_id"), F.col("name").alias("also_bought_name")),
                  "also_bought_id")
            .select("product_id", "name", "rank", "also_bought_id", "also_bought_name",
                    "pair_orders", "x_orders", "y_orders",
                    F.round("support", 6).alias("support"),
                    F.round("confidence", 4).alias("confidence"),
                    F.round("lift", 2).alias("lift")))


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("CleanAndRecommend")
             .master("local[3]")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.driver.memory", "4g")
             .config("spark.network.timeout", "600s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.sql.shuffle.partitions", "24")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # JSON Lines: one record per line, so no multiLine option
    raw = {name: spark.read.json(f"{RAW_PATH}/{name}.jsonl")
           for name in ["users", "products", "categories", "transactions"]}

    # Sessions are large: convert JSON Lines -> Parquet once, later runs reuse it
    if not os.path.exists(f"{SESSIONS_RAW_PATH}/_SUCCESS"):
        spark.read.json(f"{RAW_PATH}/sessions_*.jsonl").write.mode("overwrite").parquet(SESSIONS_RAW_PATH)
    raw["sessions"] = spark.read.parquet(SESSIONS_RAW_PATH)

    users = clean_users(raw["users"])
    products = clean_products(raw["products"])
    categories, subcategories = clean_categories(raw["categories"])
    transactions, transaction_items = clean_transactions(raw["transactions"], products)
    sessions = clean_sessions(raw["sessions"])

    print("\n=== Cleaning: write curated tables ===")
    tables = {
        "users": users, "products": products, "categories": categories,
        "subcategories": subcategories, "transactions": transactions,
        "transaction_items": transaction_items, "sessions": sessions,
    }
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{CURATED_PATH}/{name}")

    curated = {name: spark.read.parquet(f"{CURATED_PATH}/{name}") for name in tables}
    for name in ["users", "products", "categories", "transactions", "sessions"]:
        report(name, raw[name].count(), curated[name])
    raw_lines = raw["transactions"].select(F.explode("items")).count()
    report("transaction_items", raw_lines, curated["transaction_items"])
    print(f"subcategories      {curated['subcategories'].count():,} rows")

    txns = curated["transactions"]
    print("\nTransactions without a session:", txns.filter(~F.col("has_session")).count())
    print("Transactions with total mismatch:", txns.filter("total_mismatch").count())

    print("\n=== Recommendations: customers who bought X also bought Y ===")
    recs = also_bought(curated["transaction_items"], curated["products"])
    recs.write.mode("overwrite").parquet(RECOMMENDATIONS_PATH)
    recs = spark.read.parquet(RECOMMENDATIONS_PATH)

    print(f"{recs.count():,} recommendations for {recs.select('product_id').distinct().count():,} products")
    print("\nStrongest associations overall (by lift, at least 3 shared orders):")
    recs.filter("pair_orders >= 3").orderBy(F.desc("lift")).show(10, truncate=30)
    example = recs.orderBy(F.desc("x_orders")).first()["product_id"]
    print(f"Example - customers who bought {example} also bought:")
    recs.filter(F.col("product_id") == example).orderBy("rank").show(truncate=40)

    spark.stop()
