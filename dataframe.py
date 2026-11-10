import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SESSIONS_PARQUET_PATH = "./parquet/sessions"

spark = SparkSession.builder.appName("DataProcessing").master("local[3]").config("spark.hadoop.fs.defaultFS", "file:///").config("spark.driver.memory", "4g").config("spark.network.timeout","600s").config("spark.executor.heartbeatInterval","60s").getOrCreate()


users_df = spark.read.option("multiLine",True).json("./jsonl/users.jsonl")
products_df = spark.read.option("multiLine",True).json("./jsonl/products.jsonl")
categories_df = spark.read.option("multiLine",True).json("./jsonl/categories.jsonl")
transactions_df = spark.read.option("multiLine",True).json("./jsonl/transactions.jsonl")


sessions_schema = spark.read.option("multiLine",True).json("./jsonl/sessions_0.jsonl").schema

if not os.path.exists(f"{SESSIONS_PARQUET_PATH}/_SUCCESS"):
    spark.read.option("multiLine",True).schema(sessions_schema).json("./jsonl/sessions_*.jsonl").write.mode("overwrite").parquet(SESSIONS_PARQUET_PATH)

sessions_df = spark.read.parquet(SESSIONS_PARQUET_PATH)

users_df.show()
cleaned_df = users_df.dropDuplicates()
cleaned_df.show()

spark.stop()