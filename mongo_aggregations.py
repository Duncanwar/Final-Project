# # mongo_aggregations.py
# # Product popularity analytics on the ecommerce database (see mongo_model.py).
# #
# # Pipelines:
# #   1. top_selling_products  - transactions: units sold, revenue, revenue rank and
# #                              share of total revenue, enriched with product + category.
# #   2. most_viewed_products  - sessions: product detail views, unique sessions, viewing
# #                              time and session conversion rate, compared with units sold.
# #
# # Usage:
# #   python mongo_aggregations.py
# import time

# from pymongo import MongoClient

# MONGO_URI = "mongodb://localhost:27017"
# DB_NAME = "ecommerce"
# TOP_N = 10


# # --- Pipeline 1: top-selling products ---
# def top_selling_products(limit=TOP_N):
#     return [
#         # One document per order line (every status counts as a sale)
#         {"$unwind": "$items"},
#         # Totals per product
#         {"$group": {
#             "_id": "$items.product_id",
#             "units_sold": {"$sum": "$items.quantity"},
#             "revenue": {"$sum": "$items.subtotal"},
#             "orders": {"$sum": 1},
#             "avg_unit_price": {"$avg": "$items.unit_price"},
#         }},
#         # Rank every product by revenue and attach overall revenue for the share
#         {"$setWindowFields": {
#             "sortBy": {"revenue": -1},
#             "output": {
#                 "revenue_rank": {"$rank": {}},
#                 "total_revenue": {"$sum": "$revenue", "window": {"documents": ["unbounded", "unbounded"]}},
#             },
#         }},
#         {"$limit": limit},
#         # Enrich with product and category details (only for the top N)
#         {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
#         {"$unwind": "$product"},
#         {"$lookup": {"from": "categories", "localField": "product.category_id", "foreignField": "_id",
#                      "as": "category"}},
#         {"$unwind": "$category"},
#         {"$project": {
#             "_id": 0,
#             "rank": "$revenue_rank",
#             "product_id": "$_id",
#             "name": "$product.name",
#             "category": "$category.name",
#             "units_sold": 1,
#             "orders": 1,
#             "revenue": {"$round": ["$revenue", 2]},
#             "revenue_share_pct": {"$round": [{"$multiply": [{"$divide": ["$revenue", "$total_revenue"]}, 100]}, 3]},
#             "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
#             "list_price": "$product.base_price",
#         }},
#     ]


# # --- Pipeline 2: most viewed products ---
# def most_viewed_products(limit=TOP_N):
#     return [
#         # Sessions that viewed at least one product (uses the viewed_products index)
#         {"$match": {"viewed_products.0": {"$exists": True}}},
#         {"$project": {"conversion_status": 1, "page_views": 1}},
#         {"$unwind": "$page_views"},
#         {"$match": {"page_views.page_type": "product_detail"}},
#         # First group per (product, session) so a session counts once per product
#         {"$group": {
#             "_id": {"product_id": "$page_views.product_id", "session_id": "$_id"},
#             "views": {"$sum": 1},
#             "view_seconds": {"$sum": "$page_views.view_duration"},
#             "converted": {"$first": {"$eq": ["$conversion_status", "converted"]}},
#         }},
#         # Then per product
#         {"$group": {
#             "_id": "$_id.product_id",
#             "views": {"$sum": "$views"},
#             "unique_sessions": {"$sum": 1},
#             "converted_sessions": {"$sum": {"$cond": ["$converted", 1, 0]}},
#             "total_view_seconds": {"$sum": "$view_seconds"},
#         }},
#         {"$sort": {"views": -1}},
#         {"$limit": limit},
#         {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
#         {"$unwind": "$product"},
#         # Units actually sold for this product (uses the items.product_id index)
#         {"$lookup": {
#             "from": "transactions",
#             "let": {"pid": "$_id"},
#             "pipeline": [
#                 {"$match": {"$expr": {"$in": ["$$pid", "$items.product_id"]}}},
#                 {"$unwind": "$items"},
#                 {"$match": {"$expr": {"$eq": ["$items.product_id", "$$pid"]}}},
#                 {"$group": {"_id": None, "units": {"$sum": "$items.quantity"}}},
#             ],
#             "as": "sales",
#         }},
#         {"$project": {
#             "_id": 0,
#             "product_id": "$_id",
#             "name": "$product.name",
#             "views": 1,
#             "unique_sessions": 1,
#             "avg_view_seconds": {"$round": [{"$divide": ["$total_view_seconds", "$views"]}, 1]},
#             "session_conversion_pct": {"$round": [
#                 {"$multiply": [{"$divide": ["$converted_sessions", "$unique_sessions"]}, 100]}, 2]},
#             "units_sold": {"$ifNull": [{"$first": "$sales.units"}, 0]},
#         }},
#         {"$addFields": {
#             "units_per_100_views": {"$round": [{"$multiply": [{"$divide": ["$units_sold", "$views"]}, 100]}, 2]},
#         }},
#     ]


# def run(db, collection, name, pipeline):
#     start = time.perf_counter()
#     rows = list(db[collection].aggregate(pipeline, allowDiskUse=True))
#     print(f"\n=== {name} ({time.perf_counter() - start:.1f}s) ===")
#     for row in rows:
#         print(row)
#     return rows


# if __name__ == "__main__":
#     db = MongoClient(MONGO_URI)[DB_NAME]
#     run(db, "transactions", "Top-selling products by revenue", top_selling_products())
#     run(db, "sessions", "Most viewed products", most_viewed_products())
