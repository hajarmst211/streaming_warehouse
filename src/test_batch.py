from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Spark automatically reads spark-defaults.conf!
spark = (
    SparkSession.builder.master("local[*]").appName("Transactions").getOrCreate()
)

# Now your original code just works
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.warehouse")

schema = StructType(
    [
        StructField("sender_id", LongType(), True),
        StructField("reciever_id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_type", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
df.writeTo("demo.warehouse.transactions").create()