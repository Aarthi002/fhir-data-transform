from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/database.env")

# Database credentials
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Print the loaded environment variables
print("DB User:", db_user)
print("DB Password:", db_password)
print("DB Host:", db_host)
print("DB Port:", db_port)
print("DB Name:", db_name)

# Define the path to the JDBC driver
jdbc_driver_path = "libs/postgresql-42.5.6.jar"  # Update this path

# Initialize SparkSession with JDBC driver explicitly included in the classpath
spark = SparkSession.builder \
    .appName("PySpark Database Example") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

# Read sample data into a Spark DataFrame
input_path = "sample_data.csv"

if not os.path.exists(input_path):
    print(f"File not found: {input_path}")
else:
    # Read the data into the DataFrame
    df = spark.read.csv(input_path, header=True, inferSchema=True)
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Print the schema
df = spark.read.csv(input_path, header=True, inferSchema=True).limit(5)
df.show()

# Define the JDBC URL
#jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Write the DataFrame to the PostgreSQL database
print("Schema of DataFrame:")
df.printSchema()  # Print schema to verify column names and types

print("Top 5 rows of DataFrame:")
df.show(5)  # Show top 5 rows to verify if data is present

# If the DataFrame is empty, display a message and stop
if df.count() == 0:
    print("No data found in the DataFrame. Exiting.")
else:
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
        .option("dbtable", "your_table_name") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Data loaded successfully!")