# dataframe.py
# Spark batch job: clean + normalize the raw e-commerce dataset, then run a cohort
# analysis of user purchasing patterns (users grouped by registration month, spending
# followed month by month).
#
# Input:
#   ./jsonl/*.jsonl          users, products, categories, transactions, sessions (JSON Lines)
#   ./parquet/sessions       sessions converted once from JSON Lines to Parquet
# Output (Parquet, overwritten on every run):
#   ./parquet/curated/{users,products,categories,subcategories,transactions,
#                      transaction_items,sessions}
#   ./parquet/analytics/user_cohorts
#
# Usage:
#   python dataframe.py
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "./jsonl"
SESSIONS_RAW_PATH = "./parquet/sessions"
CURATED_PATH = "./parquet/curated"
COHORTS_PATH = "./parquet/analytics/user_cohorts"

MIN_DAYS_PER_MONTH = 28   # order months with fewer days of data are partial and left out


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
            .withColumn("price_changes"   , F.greatest(F.size("price_history") - 1, F.lit(0)))
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


# --- Cohort analysis: users grouped by registration month ---
def cohort_analysis(users, transactions):
    """One row per (registration cohort, order month): how many of the cohort bought,
    and how much the cohort spent, relative to the cohort's size."""
    cohorts = users.select("user_id", F.date_trunc("month", "registration_date").alias("cohort_month"))
    cohort_size = cohorts.groupBy("cohort_month").agg(F.count("*").alias("cohort_size"))

    orders = transactions.select("user_id", "total", "date",
                                 F.date_trunc("month", "timestamp").alias("order_month"))

    # The data window starts and ends mid-month; keep only (nearly) complete order months
    # so the months can be compared fairly
    full_months = (orders.groupBy("order_month")
                   .agg(F.countDistinct("date").alias("days"))
                   .filter(F.col("days") >= MIN_DAYS_PER_MONTH))

    activity = (orders.join(full_months, "order_month")
                .join(cohorts, "user_id")
                .groupBy("cohort_month", "order_month", "days")
                .agg(F.countDistinct("user_id").alias("active_customers"),
                     F.count("*").alias("orders"),
                     F.sum("total").alias("revenue")))

    return (activity.join(cohort_size, "cohort_month")
            .select(
                F.date_format("cohort_month", "yyyy-MM").alias("cohort"),
                F.date_format("order_month", "yyyy-MM").alias("order_month"),
                F.months_between("order_month", "cohort_month").cast("int").alias("months_since_registration"),
                "cohort_size",
                "active_customers",
                "orders",
                F.round("revenue", 2).alias("revenue"),
                # share of the cohort that bought at least once in that month
                F.round(100 * F.col("active_customers") / F.col("cohort_size"), 1).alias("active_pct"),
                # spend per registered user, so cohorts of different sizes compare fairly
                F.round(F.col("revenue") / F.col("cohort_size"), 2).alias("revenue_per_user"),
                # per day of data, so a 29-day month compares fairly with a 31-day month
                F.col("days").alias("days_of_data"),
                F.round(F.col("revenue") / F.col("cohort_size") / F.col("days"), 2).alias("revenue_per_user_per_day"),
                F.round(F.col("orders") / F.col("cohort_size"), 2).alias("orders_per_user"),
                F.round(F.col("revenue") / F.col("orders"), 2).alias("avg_order_value"))
            .orderBy("cohort", "order_month"))


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("CleanAndCohorts")
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

    print("\n=== Cohort analysis: users by registration month ===")
    cohorts = cohort_analysis(curated["users"], curated["transactions"])
    cohorts.coalesce(1).write.mode("overwrite").parquet(COHORTS_PATH)
    cohorts = spark.read.parquet(COHORTS_PATH).orderBy("cohort", "order_month")
    cohorts.show(50, truncate=False)

    for metric, title in [("active_pct", "Share of each cohort that bought in the month (%)"),
                          ("revenue_per_user", "Spend per registered user in the month ($)"),
                          ("revenue_per_user_per_day", "Spend per registered user per day of data ($)")]:
        print(title)
        (cohorts.groupBy("cohort", "cohort_size").pivot("order_month").agg(F.first(metric))
         .orderBy("cohort").show(truncate=False))

    print("Whole window, per cohort:")
    (cohorts.groupBy("cohort", "cohort_size")
     .agg(F.sum("revenue").alias("revenue"), F.sum("orders").alias("orders"))
     .select("cohort", "cohort_size",
             F.round(F.col("revenue") / F.col("cohort_size"), 2).alias("revenue_per_user"),
             F.round(F.col("orders") / F.col("cohort_size"), 2).alias("orders_per_user"))
     .orderBy("cohort").show(truncate=False))

    spark.stop()
