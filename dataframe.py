from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.functions import input_file_name

spark = SparkSession.builder.appName("DataProcessing").master("local[3]").config("spark.hadoop.fs.defaultFS", "file:///").config("spark.driver.memory", "4g").getOrCreate()

users_df = spark.read.option("multiLine",True).json("./users.json")
products_df = spark.read.option("multiLine",True).json("./products.json")
categories_df = spark.read.option("multiLine",True).json("./categories.json")
transactions_df = spark.read.option("multiLine",True).json("./transactions.json")
sample = spark.read.option("multiLine",True).json("./sessions_0.json")
sessions_schema = sample.schema
sessions_df= spark.read.option("multiLine",True).json("./sessions_*.json")
sessions_df.write.mode("overwrite").parquet("./sessions_*.parquet")


users_df.show()
cleaned_df = users_df.dropDuplicates()
cleaned_df.show()