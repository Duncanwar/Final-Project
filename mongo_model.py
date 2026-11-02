# mongo_model.py
# MongoDB data model for the synthetic e-commerce dataset.
#
# Design:
#   - Each entity is a collection; its natural id (user_id, product_id, ...) is used as _id.
#   - Data that is always read together is embedded:
#       categories   -> subcategories
#       products     -> price_history
#       sessions     -> page_views, cart_contents, geo_data, device_profile
#       transactions -> items
#   - Relationships across collections are references by id
#     (sessions.user_id, transactions.session_id, products.category_id, ...).
#   - ISO timestamp strings are converted to real dates so time-based queries work.
#
# Usage:
#   pip install pymongo  
#   python mongo_model.py            # create collections + indexes, load all JSON files
import glob
import json
import datetime

from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce"
BATCH_SIZE = 10000      

# --- Schema validators ---
SCHEMAS = {
    "categories": {
        "bsonType": "object",
        "required": ["_id", "name", "subcategories"],
        "properties": {
            "_id": {"bsonType": "string"},
            "name": {"bsonType": "string"},
            "subcategories": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["subcategory_id", "name", "profit_margin"],
                    "properties": {
                        "subcategory_id": {"bsonType": "string"},
                        "name": {"bsonType": "string"},
                        "profit_margin": {"bsonType": "double"},
                    },
                },
            },
        },
    },
    "products": {
        "bsonType": "object",
        "required": ["_id", "name", "category_id", "base_price", "current_stock", "is_active"],
        "properties": {
            "_id": {"bsonType": "string"},
            "name": {"bsonType": "string"},
            "category_id": {"bsonType": "string"},
            "base_price": {"bsonType": "double"},
            "current_stock": {"bsonType": "int", "minimum": 0},
            "is_active": {"bsonType": "bool"},
            "creation_date": {"bsonType": "date"},
            "price_history": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["price", "date"],
                    "properties": {
                        "price": {"bsonType": "double"},
                        "date": {"bsonType": "date"},
                    },
                },
            },
        },
    },
    "users": {
        "bsonType": "object",
        "required": ["_id", "geo_data", "registration_date"],
        "properties": {
            "_id": {"bsonType": "string"},
            "geo_data": {
                "bsonType": "object",
                "properties": {
                    "city": {"bsonType": "string"},
                    "state": {"bsonType": "string"},
                    "country": {"bsonType": "string"},
                },
            },
            "registration_date": {"bsonType": "date"},
            "last_active": {"bsonType": "date"},
        },
    },
    "sessions": {
        "bsonType": "object",
        "required": ["_id", "user_id", "start_time", "end_time", "conversion_status"],
        "properties": {
            "_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "start_time": {"bsonType": "date"},
            "end_time": {"bsonType": "date"},
            "duration_seconds": {"bsonType": "int"},
            "geo_data": {"bsonType": "object"},
            "device_profile": {"bsonType": "object"},
            "viewed_products": {"bsonType": "array", "items": {"bsonType": "string"}},
            "page_views": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["timestamp", "page_type"],
                    "properties": {
                        "timestamp": {"bsonType": "date"},
                        "page_type": {"enum": ["home", "search", "category_listing", "product_detail",
                                               "cart", "checkout", "confirmation"]},
                        "product_id": {"bsonType": ["string", "null"]},
                        "category_id": {"bsonType": ["string", "null"]},
                        "view_duration": {"bsonType": "int"},
                    },
                },
            },
            "cart_contents": {"bsonType": "object"},
            "conversion_status": {"enum": ["converted", "abandoned", "browsed"]},
            "referrer": {"bsonType": "string"},
        },
    },
    "transactions": {
        "bsonType": "object",
        "required": ["_id", "user_id", "timestamp", "items", "total", "status"],
        "properties": {
            "_id": {"bsonType": "string"},
            "session_id": {"bsonType": ["string", "null"]},
            "user_id": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "items": {
                "bsonType": "array",
                "minItems": 1,
                "items": {
                    "bsonType": "object",
                    "required": ["product_id", "quantity", "unit_price", "subtotal"],
                    "properties": {
                        "product_id": {"bsonType": "string"},
                        "quantity": {"bsonType": "int", "minimum": 1},
                        "unit_price": {"bsonType": "double"},
                        "subtotal": {"bsonType": "double"},
                    },
                },
            },
            "subtotal": {"bsonType": "double"},
            "discount": {"bsonType": ["double", "int"]},
            "total": {"bsonType": "double"},
            "payment_method": {"bsonType": "string"},
            "status": {"enum": ["completed", "processing", "shipped", "delivered"]},
        },
    },
} 

# --- Indexes (chosen for the typical analysis queries) ---
INDEXES = {
    "products": [[("category_id", ASCENDING)], [("is_active", ASCENDING)]],
    "users": [[("geo_data.country", ASCENDING)], [("registration_date", ASCENDING)]],
    "sessions": [
        [("user_id", ASCENDING), ("start_time", DESCENDING)],
        [("start_time", ASCENDING)],
        [("conversion_status", ASCENDING)],
        [("viewed_products", ASCENDING)],
    ],
    "transactions": [
        [("user_id", ASCENDING), ("timestamp", DESCENDING)],
        [("timestamp", ASCENDING)],
        [("session_id", ASCENDING)],
        [("items.product_id", ASCENDING)],
    ],
}

# Which field becomes _id, and which fields hold ISO timestamps, per collection
ID_FIELDS = {
    "categories": "category_id",
    "products": "product_id",
    "users": "user_id",
    "sessions": "session_id",
    "transactions": "transaction_id",
}
DATE_FIELDS = {
    "products": ["creation_date"],
    "users": ["registration_date", "last_active"],
    "sessions": ["start_time", "end_time"],
    "transactions": ["timestamp"],
}


# --- Helpers ---
def to_date(value):
    return datetime.datetime.fromisoformat(value) if isinstance(value, str) else value


def prepare(collection, doc):
    """Turn a raw JSON record into a MongoDB document."""
    doc["_id"] = doc.pop(ID_FIELDS[collection])
    for field in DATE_FIELDS.get(collection, []):
        if field in doc:
            doc[field] = to_date(doc[field])

    if collection == "products":
        for entry in doc["price_history"]:
            entry["date"] = to_date(entry["date"])
            entry["price"] = float(entry["price"])
        doc["base_price"] = float(doc["base_price"])
    elif collection == "sessions":
        for view in doc["page_views"]:
            view["timestamp"] = to_date(view["timestamp"])
    elif collection == "transactions":
        for item in doc["items"]:
            item["unit_price"] = float(item["unit_price"])
            item["subtotal"] = float(item["subtotal"])
        doc["subtotal"] = float(doc["subtotal"])
        doc["total"] = float(doc["total"])
    elif collection == "categories":
        for sub in doc["subcategories"]:
            sub["profit_margin"] = float(sub["profit_margin"])
    return doc


# --- Setup ---
def create_collections(db, drop_existing=False):
    for name, schema in SCHEMAS.items():
        if drop_existing:
            db.drop_collection(name)
        if name not in db.list_collection_names():
            db.create_collection(name, validator={"$jsonSchema": schema}, validationLevel="moderate")
        for keys in INDEXES.get(name, []):
            db[name].create_index(keys)
    print("Collections and indexes ready")


def load_file(db, collection, path):
    with open(path) as f:
        records = json.load(f)

    inserted = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = [prepare(collection, r) for r in records[i:i + BATCH_SIZE]]
        result = db[collection].insert_many(batch, ordered=False)
        inserted += len(result.inserted_ids)
    print(f"{path}: {inserted:,} documents -> {collection}")


def load_all(db):
    load_file(db, "categories", "categories.json")
    load_file(db, "products", "products.json")
    load_file(db, "users", "users.json")
    load_file(db, "transactions", "transactions.json")
    # Session files are loaded one at a time to keep memory usage down
    for path in sorted(glob.glob("sessions_*.json"), key=lambda p: int(p.split("_")[1].split(".")[0])):
        load_file(db, "sessions", path)


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    create_collections(db, drop_existing=True)
    load_all(db)
    for name in SCHEMAS:
        print(f"{name}: {db[name].estimated_document_count():,}")
