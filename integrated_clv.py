# integrated_clv.py
# Integrated analytical query: Customer Lifetime Value (CLV) estimation across stores.
#
#   MongoDB  users          -> profile (country, registration date)
#   MongoDB  transactions   -> order history, aggregated per customer inside MongoDB
#   MongoDB  categories     -> average profit margin
#   HBase    user_sessions  -> engagement (session frequency, duration, conversion)
#
# The sources are joined and scored in one Spark SQL query.
#
# CLV model (simple, transparent):
#   monthly value   = average order value x orders per month x profit margin
#   monthly churn   = share of customers with no order in the last CHURN_DAYS of the window
#   lifespan        = 1 / monthly churn, capped at MAX_LIFESPAN_MONTHS
#   CLV             = monthly value x lifespan
#   Session counts come from the HBase sample, so they are scaled up by 1 / sample fraction.
#
# Output: ./parquet/analytics/clv_users, ./parquet/analytics/clv_segments
#
# Usage:
#   python integrated_clv.py
from data_sources import (get_spark, save, mongo_users, mongo_customer_orders,
                          mongo_avg_profit_margin, hbase_sessions, HBASE_SAMPLE_FRACTION)

CHURN_DAYS = 30
MAX_LIFESPAN_MONTHS = 36

CLV_QUERY = """
WITH observation AS (
    SELECT MIN(first_order)                                         AS window_start,
           MAX(last_order)                                          AS window_end,
           GREATEST(datediff(MAX(last_order), MIN(first_order)), 1) / 30.0 AS months
    FROM customer_orders
),
churn AS (
    -- customers whose last order is older than CHURN_DAYS count as churned
    SELECT GREATEST(AVG(CASE WHEN datediff(w.window_end, o.last_order) > {churn_days} THEN 1.0 ELSE 0.0 END),
                    1.0 / {max_lifespan})                           AS monthly_churn
    FROM customer_orders o CROSS JOIN observation w
),
engagement AS (
    SELECT user_id,
           COUNT(*)                                                         AS sampled_sessions,
           AVG(duration_seconds) / 60                                       AS avg_session_minutes,
           AVG(CASE WHEN conversion_status = 'converted' THEN 1.0 ELSE 0.0 END) AS session_conversion
    FROM sessions
    GROUP BY user_id
),
customers AS (
    SELECT u.user_id,
           u.country,
           u.registration_date,
           COALESCE(o.orders, 0)                                    AS orders,
           ROUND(COALESCE(o.revenue, 0), 2)                         AS revenue,
           o.first_order,
           o.last_order,
           datediff(w.window_end, o.last_order)                     AS recency_days,
           o.revenue / o.orders                                     AS avg_order_value,
           COALESCE(o.orders, 0) / w.months                         AS orders_per_month,
           COALESCE(e.sampled_sessions, 0) / {sample_fraction} / w.months AS sessions_per_month,
           e.avg_session_minutes,
           e.session_conversion
    FROM users u
    LEFT JOIN customer_orders o ON o.user_id = u.user_id
    LEFT JOIN engagement e      ON e.user_id = u.user_id
    CROSS JOIN observation w
),
scored AS (
    SELECT c.*,
           COALESCE(c.avg_order_value * c.orders_per_month * {margin}, 0)  AS monthly_value,
           1 / ch.monthly_churn                                            AS expected_lifespan_months
    FROM customers c CROSS JOIN churn ch
)
SELECT *,
       ROUND(monthly_value * expected_lifespan_months, 2)                  AS clv,
       CASE WHEN orders = 0 THEN 'No purchases'
            ELSE element_at(array('Bronze', 'Silver', 'Gold', 'Platinum'),
                            NTILE(4) OVER (PARTITION BY orders > 0 ORDER BY monthly_value))
       END                                                                 AS segment
FROM scored
"""

SEGMENT_SUMMARY = """
SELECT segment,
       COUNT(*)                                  AS customers,
       ROUND(AVG(clv), 2)                        AS avg_clv,
       ROUND(SUM(clv) / SUM(SUM(clv)) OVER () * 100, 1) AS clv_share_pct,
       ROUND(AVG(revenue), 2)                    AS avg_revenue_to_date,
       ROUND(AVG(avg_order_value), 2)            AS avg_order_value,
       ROUND(AVG(orders_per_month), 2)           AS orders_per_month,
       ROUND(AVG(sessions_per_month), 2)         AS sessions_per_month,
       ROUND(AVG(avg_session_minutes), 1)        AS avg_session_minutes,
       ROUND(100 * AVG(session_conversion), 2)   AS session_conversion_pct,
       ROUND(AVG(recency_days), 1)               AS avg_recency_days
FROM clv_users
GROUP BY segment
ORDER BY avg_clv DESC
"""


if __name__ == "__main__":
    spark = get_spark("IntegratedCLV")

    print("Loading sources")
    mongo_users(spark).createOrReplaceTempView("users")
    mongo_customer_orders(spark).createOrReplaceTempView("customer_orders")
    hbase_sessions(spark).createOrReplaceTempView("sessions")
    margin = mongo_avg_profit_margin()
    print(f"  average profit margin: {margin:.1%}")

    clv = spark.sql(CLV_QUERY.format(churn_days=CHURN_DAYS, max_lifespan=MAX_LIFESPAN_MONTHS,
                                     sample_fraction=HBASE_SAMPLE_FRACTION, margin=margin))
    clv = save(clv, "clv_users")
    clv.createOrReplaceTempView("clv_users")

    lifespan = clv.select("expected_lifespan_months").first()[0]
    print(f"\nModel: monthly churn {1 / lifespan:.1%} -> expected lifespan {lifespan:.1f} months")

    print("\n=== CLV by segment ===")
    save(spark.sql(SEGMENT_SUMMARY), "clv_segments").orderBy("avg_clv", ascending=False).show(truncate=False)

    print("=== Top 10 customers by CLV ===")
    clv.orderBy("clv", ascending=False).selectExpr(
        "user_id", "country", "orders", "revenue", "ROUND(avg_order_value, 2) AS avg_order_value",
        "ROUND(orders_per_month, 1) AS orders_per_month", "ROUND(sessions_per_month, 1) AS sessions_per_month",
        "clv", "segment").show(10)

    print("=== Does engagement go with value? (Pearson correlation with CLV, buyers only) ===")
    buyers = clv.filter("orders > 0")
    for col in ["sessions_per_month", "avg_session_minutes", "session_conversion", "orders_per_month"]:
        print(f"  {col:<22} r = {buyers.dropna(subset=[col]).stat.corr(col, 'clv'):+.3f}")

    spark.stop()
