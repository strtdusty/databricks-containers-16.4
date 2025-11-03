from pyspark.sql import SparkSession
import sys

# Create Spark session
spark = SparkSession.builder \
    .appName("SelectFromTable") \
    .getOrCreate()

# Get the table name to select from the command line argument
table_name = sys.argv[1]

print(f"Selecting from table: {table_name}")

df = spark.sql(f"SELECT * FROM {table_name}")

# Show the results
df.show()

# Stop the Spark session
spark.stop()

print("Exiting the system now.")
spark.sparkContext._gateway.jvm.System.exit(0)
