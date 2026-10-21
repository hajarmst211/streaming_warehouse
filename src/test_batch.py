from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

spark = (
    SparkSession.builder.master("local[*]").appName("Transactions").getOrCreate()
)

# Now your original code just works
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.finance")

schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
    ]
)

df = spark.createDataFrame([], schema)
'''
demo: name of the MinIOn catalog connection in Spark
finance: Namespace/Database- a logical folder to organize related tables
transactions: the actual Iceberg table/dataset created
'''
df.writeTo("demo.finance.transactions").createOrReplace()