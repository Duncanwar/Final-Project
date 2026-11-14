# mongo_aggregations.py
# Two MongoDB aggregation pipelines on the ecommerce database (see mongo_model.py):
#   1. most viewed products        - sessions: product detail page views per product
#   2. most frequent buyers        - transactions: completed orders per user
#
# Usage:
#   python mongo_aggregations.py
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce"
TOP_N = 10

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# 1. Most viewed products
pipeline = [
    {"$unwind": "$page_views"},                                  # one document per page view
    {"$match": {"page_views.page_type": "product_detail"}},      # keep product pages only
    {"$group": {
        "_id": "$page_views.product_id",
        "total_views": {"$sum": 1}
    }},
    {"$sort": {"total_views": -1}},
    {"$limit": TOP_N}
]

for row in db.sessions.aggregate(pipeline):
    print(row)

# 2. Most frequent buyers
pipeline2 = [
    {"$match": {"status": "completed"}},
    {"$group": {
        "_id": "$user_id",
        "purchase_frequency": {"$sum": 1}
    }},
    {"$sort": {"purchase_frequency": -1}},
    {"$limit": TOP_N}
]

for row in db.transactions.aggregate(pipeline2):
    print(row)
