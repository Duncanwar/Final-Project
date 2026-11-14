# integrated_recommendations.py
# Integrated analytical query: product affinity / recommendations across stores.
#
# Business question: on each product page, which other products should we suggest?
#
#   MongoDB  transactions   -> which products are BOUGHT together (same order)
#   HBase    user_sessions  -> which products are VIEWED together (same session, views family)
#   MongoDB  products       -> names and active flag of the suggested products
#
# Spark joins the two signals in one SQL query. For a product P and a candidate Q:
#   buy_confidence  = orders containing P and Q   / orders containing P
#   view_confidence = sessions viewing P and Q    / sessions viewing P
#   lift            = confidence / overall share of Q (> 1 means more than chance)
#   score           = BUY_WEIGHT * buy_confidence + VIEW_WEIGHT * view_confidence
# Purchases weigh more because they are a stronger sign of intent than views.
# The TOP_K candidates with the highest score are kept per product (inactive products
# are never suggested).
#
# Output: ./parquet/analytics/product_affinity
#
# Usage:
#   python integrated_recommendations.py
from data_sources import (get_spark, save, mongo_order_baskets, mongo_products,
                          hbase_product_views)

BUY_WEIGHT, VIEW_WEIGHT = 0.6, 0.4
TOP_K = 5
MIN_PAIR_ORDERS, MIN_PAIR_SESSIONS = 2, 2   # ignore pairs seen together only once

AFFINITY_QUERY = """
WITH b AS (SELECT DISTINCT transaction_id, product_id FROM baskets),
v AS (SELECT DISTINCT session_id, product_id FROM views),
totals AS (
    SELECT (SELECT COUNT(DISTINCT transaction_id) FROM b) AS n_orders,
           (SELECT COUNT(DISTINCT session_id) FROM v)     AS n_sessions
),
bought AS (SELECT product_id, COUNT(*) AS orders        FROM b GROUP BY product_id),
viewed AS (SELECT product_id, COUNT(*) AS view_sessions FROM v GROUP BY product_id),
co_buy AS (       -- MongoDB: bought in the same order
    SELECT x.product_id AS p, y.product_id AS q, COUNT(*) AS pair_orders
    FROM b x JOIN b y ON x.transaction_id = y.transaction_id AND x.product_id <> y.product_id
    GROUP BY x.product_id, y.product_id
),
co_view AS (      -- HBase: viewed in the same session
    SELECT x.product_id AS p, y.product_id AS q, COUNT(*) AS pair_sessions
    FROM v x JOIN v y ON x.session_id = y.session_id AND x.product_id <> y.product_id
    GROUP BY x.product_id, y.product_id
),
pairs AS (
    SELECT COALESCE(cb.p, cv.p)            AS product_id,
           COALESCE(cb.q, cv.q)            AS suggested_id,
           COALESCE(cb.pair_orders, 0)     AS pair_orders,
           COALESCE(cv.pair_sessions, 0)   AS pair_sessions
    FROM co_buy cb FULL OUTER JOIN co_view cv ON cb.p = cv.p AND cb.q = cv.q
    WHERE COALESCE(cb.pair_orders, 0) >= {min_orders} OR COALESCE(cv.pair_sessions, 0) >= {min_sessions}
),
scored AS (
    SELECT pr.*,
           COALESCE(pr.pair_orders / bp.orders, 0)          AS buy_confidence,
           COALESCE(pr.pair_sessions / vp.view_sessions, 0) AS view_confidence,
           (pr.pair_orders / bp.orders) / (bq.orders / t.n_orders)                 AS buy_lift,
           (pr.pair_sessions / vp.view_sessions) / (vq.view_sessions / t.n_sessions) AS view_lift
    FROM pairs pr
    CROSS JOIN totals t
    LEFT JOIN bought bp ON bp.product_id = pr.product_id
    LEFT JOIN bought bq ON bq.product_id = pr.suggested_id
    LEFT JOIN viewed vp ON vp.product_id = pr.product_id
    LEFT JOIN viewed vq ON vq.product_id = pr.suggested_id
),
ranked AS (
    SELECT s.*,
           {buy_w} * buy_confidence + {view_w} * view_confidence AS score,
           ROW_NUMBER() OVER (PARTITION BY s.product_id
                              ORDER BY {buy_w} * buy_confidence + {view_w} * view_confidence DESC,
                                       s.suggested_id) AS rank
    FROM scored s
    JOIN products q ON q.product_id = s.suggested_id AND q.is_active   -- never suggest inactive items
)
SELECT r.product_id, p.name, r.rank, r.suggested_id, q.name AS suggested_name,
       r.pair_orders, r.pair_sessions,
       ROUND(r.buy_confidence, 4)  AS buy_confidence,
       ROUND(r.view_confidence, 4) AS view_confidence,
       ROUND(r.buy_lift, 2)        AS buy_lift,
       ROUND(r.view_lift, 2)       AS view_lift,
       ROUND(r.score, 4)           AS score,
       CASE WHEN r.pair_orders >= {min_orders} AND r.pair_sessions >= {min_sessions} THEN 'both'
            WHEN r.pair_orders >= {min_orders} THEN 'bought together'
            ELSE 'viewed together' END AS evidence
FROM ranked r
JOIN products p ON p.product_id = r.product_id
JOIN products q ON q.product_id = r.suggested_id
WHERE r.rank <= {top_k}
"""

SUMMARY = """
SELECT evidence,
       COUNT(*)                         AS recommendations,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct,
       ROUND(AVG(buy_lift), 2)          AS avg_buy_lift,
       ROUND(AVG(view_lift), 2)         AS avg_view_lift,
       ROUND(AVG(score), 4)             AS avg_score
FROM product_affinity
GROUP BY evidence
ORDER BY recommendations DESC
"""


if __name__ == "__main__":
    spark = get_spark("IntegratedRecommendations")

    print("Loading sources")
    mongo_order_baskets(spark).createOrReplaceTempView("baskets")
    hbase_product_views(spark).createOrReplaceTempView("views")
    mongo_products(spark).cache().createOrReplaceTempView("products")

    recs = spark.sql(AFFINITY_QUERY.format(buy_w=BUY_WEIGHT, view_w=VIEW_WEIGHT, top_k=TOP_K,
                                           min_orders=MIN_PAIR_ORDERS, min_sessions=MIN_PAIR_SESSIONS))
    recs = save(recs, "product_affinity")
    recs.createOrReplaceTempView("product_affinity")

    n_products = recs.select("product_id").distinct().count()
    print(f"\n{recs.count():,} recommendations for {n_products:,} products")

    print("\n=== Which signal supports each recommendation ===")
    spark.sql(SUMMARY).show(truncate=False)

    print("=== Strongest recommendations overall ===")
    recs.orderBy("score", ascending=False).select(
        "product_id", "suggested_id", "pair_orders", "pair_sessions",
        "buy_lift", "view_lift", "score", "evidence").show(10, truncate=False)

    example = spark.sql("SELECT product_id FROM baskets GROUP BY product_id "
                        "ORDER BY COUNT(*) DESC LIMIT 1").first()[0]
    print(f"=== Example: product page of the best seller {example} ===")
    recs.filter(f"product_id = '{example}'").orderBy("rank").select(
        "rank", "suggested_id", "suggested_name", "pair_orders", "pair_sessions",
        "score", "evidence").show(truncate=40)

    spark.stop()
