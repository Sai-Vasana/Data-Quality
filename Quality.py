from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, countDistinct

# Initialize Spark
spark = SparkSession.builder.appName("DataQualityAutoFix").getOrCreate()

# Sample data
data = [
    ("Sai", 25, "M", 50000.0),
    ("Jack", None, "M", 85000.0),
    ("Jack", None, "M", 85000.0),  # duplicate
    ("Healy", 150, "F", 120000.0),  # age out of range
    ("David", 30, None, 95000.0),  # null gender
    ("Eva", 28, "F", None),  # null salary
]

columns = ["name", "age", "gender", "salary"]
df = spark.createDataFrame(data, columns)

# Drop duplicates
df = df.dropDuplicates()

# Fix nulls
df = df.fillna({
    "age": 0,               
    "gender": "Unknown",    
    "salary": 50000.0       
})

# Enforce data type (cast)
df = df.withColumn("age", col("age").cast("int"))
df = df.withColumn("salary", col("salary").cast("double"))

# Fix out-of-range values (e.g age)
df = df.withColumn("age", when((col("age") < 0) | (col("age") > 100), lit(30)).otherwise(col("age")))

# Show cleaned data
df.show()
