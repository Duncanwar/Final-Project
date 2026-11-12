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

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce"
TOP_N = 10

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

pipeline =[
         
    {"$unwind": "$page_views"},
             {"$match": { "page_views.page_type": "product_detail"}},
    {"$group": {
        "_id": "$page_views.product_id",
        "total_views": { "$sum": 1 }
    }},
    {"$sort": { "total_views": -1 }},
    {"$limit": TOP_N}
]

for row in db.sessions.aggregate(pipeline):
    print(row)
    
pipeline2 = [
    {"$match": {"status":"completed"}},
    {"$group": {
        "_id":"$user_id",
        "purchase_frequency": {"$sum": 1}
    }},
     {"$sort": { "purchase_frequency": -1 }},
        {"$limit": TOP_N}
]

for row in db.transactions.aggregate(pipeline2):
    print(row)