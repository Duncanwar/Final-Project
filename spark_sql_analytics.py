# spark_sql_analytics.py
# Spark SQL analytics.
#
# Part A - complex queries on DataFrames derived from the JSON files (curated Parquet
#          written by dataframe.py), registered as SQL views:
#   A1 monthly revenue per category: share of month, rank, month-over-month growth
#   A2 daily revenue with 7-day moving average
#   A3 session funnel by device and referrer (GROUPING SETS for subtotals)
#   A4 product performance: most viewed vs top selling
#
# Part B - the same SQL over data that lives in the operational stores: samples are
#          loaded from MongoDB (users, orders) and HBase (sessions) into DataFrames,
#          registered as views and joined in one query.
#   B1 revenue and engagement per country      (MongoDB users + orders, HBase sessions)
#   B2 order revenue attributed to device/referrer (MongoDB orders -> HBase sessions)
#
# Results used by visualizations.py are saved under ./parquet/analytics/.
#
# Usage:
#   python spark_sql_analytics.py
from data_sources import (get_spark, register_curated, save, mongo_users,
                          mongo_transactions_sample, hbase_sessions)


def run(spark, title, sql, save_as=None, rows=10):
    print(f"\n=== {title} ===")
    df = spark.sql(sql)
    if save_as:
        df = save(df, save_as)
    if rows:
        df.show(rows, truncate=28)
    return df


# --- Part A: complex queries on the JSON-derived DataFrames ---
A1_CATEGORY_MONTHLY = """
WITH monthly AS (
    SELECT date_trunc('month', ti.timestamp)    AS month,
           c.name                               AS category,
           SUM(ti.subtotal)                     AS revenue,
           SUM(ti.quantity)                     AS units,
           COUNT(DISTINCT ti.transaction_id)    AS orders
    FROM transaction_items ti
    JOIN products   p ON p.product_id  = ti.product_id
    JOIN categories c ON c.category_id = p.category_id
    GROUP BY 1, 2
)
SELECT date_format(month, 'yyyy-MM')                                      AS month,
       category,
       ROUND(revenue, 2)                                                  AS revenue,
       units,
       orders,
       ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY month), 2)  AS share_pct,
       RANK() OVER (PARTITION BY month ORDER BY revenue DESC)             AS rank_in_month,
       ROUND(100 * (revenue / LAG(revenue) OVER (PARTITION BY category ORDER BY month) - 1), 1)
                                                                          AS mom_growth_pct
FROM monthly
ORDER BY month, rank_in_month
"""

A2_DAILY_REVENUE = """
SELECT date,
       COUNT(*)                     AS orders,
       ROUND(SUM(total), 2)         AS revenue,
       ROUND(AVG(total), 2)         AS avg_order_value,
       ROUND(AVG(SUM(total)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
                                    AS revenue_7d_avg
FROM transactions
GROUP BY date
ORDER BY date
"""

A3_FUNNEL = """
SELECT CASE WHEN grouping(device_type) = 1 THEN 'ALL' ELSE device_type END AS device,
       CASE WHEN grouping(referrer)    = 1 THEN 'ALL' ELSE referrer    END AS traffic_source,
       grouping_id()                                                         AS subtotal_level,
       COUNT(*)                                                                    AS sessions,
       ROUND(100 * AVG(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END), 2) AS conversion_pct,
       ROUND(100 * AVG(CASE WHEN conversion_status = 'abandoned' THEN 1 ELSE 0 END), 2) AS abandoned_pct,
       ROUND(AVG(duration_seconds) / 60, 1)                                        AS avg_minutes,
       ROUND(AVG(page_view_count), 1)                                              AS avg_page_views
FROM sessions
GROUP BY GROUPING SETS ((device_type, referrer), (device_type), (referrer), ())
ORDER BY subtotal_level DESC, conversion_pct DESC
"""

A4_PRODUCT_PERFORMANCE = """
WITH views AS (
    SELECT product_id, COUNT(*) AS sessions_viewed
    FROM sessions LATERAL VIEW explode(viewed_products) v AS product_id
    GROUP BY product_id
),
sales AS (
    SELECT product_id,
           SUM(quantity)                  AS units_sold,
           SUM(subtotal)                  AS revenue,
           COUNT(DISTINCT transaction_id) AS orders
    FROM transaction_items
    GROUP BY product_id
)
SELECT p.product_id,
       p.name,
       c.name                                               AS category,
       COALESCE(v.sessions_viewed, 0)                       AS sessions_viewed,
       COALESCE(s.units_sold, 0)                            AS units_sold,
       ROUND(COALESCE(s.revenue, 0), 2)                     AS revenue,
       ROUND(100 * s.units_sold / v.sessions_viewed, 2)     AS units_per_100_views,
       RANK() OVER (ORDER BY COALESCE(s.units_sold, 0) DESC)     AS sales_rank,
       RANK() OVER (ORDER BY COALESCE(v.sessions_viewed, 0) DESC) AS views_rank
FROM products p
JOIN categories c  ON c.category_id = p.category_id
LEFT JOIN views v  ON v.product_id  = p.product_id
LEFT JOIN sales s  ON s.product_id  = p.product_id
"""

# --- Part B: SQL over data loaded from MongoDB and HBase ---
B1_COUNTRY = """
WITH orders AS (
    SELECT u.country,
           COUNT(*)                    AS sampled_orders,
           COUNT(DISTINCT o.user_id)   AS buyers,
           SUM(o.total)                AS revenue,
           AVG(o.total)                AS avg_order_value
    FROM mongo_orders o
    JOIN mongo_users u ON u.user_id = o.user_id
    GROUP BY u.country
),
engagement AS (
    SELECT u.country,
           COUNT(*)                                                         AS sampled_sessions,
           AVG(s.duration_seconds) / 60                                     AS avg_minutes,
           AVG(CASE WHEN s.conversion_status = 'converted' THEN 1 ELSE 0 END) AS conversion_rate
    FROM hbase_sessions s
    JOIN mongo_users u ON u.user_id = s.user_id
    GROUP BY u.country
)
SELECT o.country,
       o.buyers,
       o.sampled_orders,
       ROUND(o.revenue, 2)                    AS revenue,
       ROUND(o.avg_order_value, 2)            AS avg_order_value,
       e.sampled_sessions,
       ROUND(e.avg_minutes, 1)                AS avg_session_minutes,
       ROUND(100 * e.conversion_rate, 2)      AS conversion_pct,
       ROUND(o.revenue / e.sampled_sessions, 2) AS revenue_per_session
FROM orders o
JOIN engagement e ON e.country = o.country
ORDER BY revenue DESC
"""

B2_ATTRIBUTION = """
SELECT s.device_type,
       s.referrer,
       COUNT(*)                   AS orders,
       ROUND(SUM(o.total), 2)     AS revenue,
       ROUND(AVG(o.total), 2)     AS avg_order_value,
       ROUND(AVG(s.duration_seconds) / 60, 1) AS avg_session_minutes
FROM mongo_orders o
JOIN hbase_sessions s ON s.session_id = o.session_id
GROUP BY s.device_type, s.referrer
ORDER BY revenue DESC
"""


if __name__ == "__main__":
    spark = get_spark("SparkSQLAnalytics")

    print("Part A - views over curated JSON-derived data")
    register_curated(spark, ["users", "products", "categories", "transactions",
                             "transaction_items", "sessions"])
    run(spark, "A1 Monthly revenue per category", A1_CATEGORY_MONTHLY, "category_monthly")
    run(spark, "A2 Daily revenue (7-day moving average)", A2_DAILY_REVENUE, "daily_revenue")
    run(spark, "A3 Session funnel by device / referrer", A3_FUNNEL, "funnel", rows=40)
    products = run(spark, "A4 Product performance", A4_PRODUCT_PERFORMANCE, "product_performance", rows=0)
    print("Top 10 by units sold:")
    products.orderBy("sales_rank").show(10, truncate=28)
    print("Top 10 by sessions viewed:")
    products.orderBy("views_rank").show(10, truncate=28)

    print("\nPart B - views over data loaded from MongoDB and HBase")
    mongo_users(spark).createOrReplaceTempView("mongo_users")
    mongo_transactions_sample(spark).cache().createOrReplaceTempView("mongo_orders")
    hbase_sessions(spark).cache().createOrReplaceTempView("hbase_sessions")
    run(spark, "B1 Revenue and engagement per country (MongoDB + HBase)", B1_COUNTRY, "country_cross_store")
    run(spark, "B2 Order revenue by session device / referrer (MongoDB -> HBase)", B2_ATTRIBUTION,
        "attribution_cross_store", rows=20)

    spark.stop()
