from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, explode, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F





# Load environment variables from the .env file
load_dotenv("config/database.env")

# Database credentials from environment variables
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Check if environment variables are loaded
print("DB User:", db_user)
print("DB Host:", db_host)
print("DB Port:", db_port)
print("DB Name:", db_name)

# Define the path to the JDBC driver
jdbc_driver_path = "libs/postgresql-42.5.6.jar"  # Update this path if needed

# Initialize SparkSession with the JDBC driver
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FHIR JSON to PostgreSQL") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()
# Define the path to the JSON file
json_file_path = "data/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json"

# Read the JSON file into a DataFrame
if not os.path.exists(json_file_path):
    print(f"File not found: {json_file_path}")
    spark.stop()
    exit()

fhir_df = spark.read.json(json_file_path, multiLine=True)




# JDBC URL
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Define common write properties
write_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Write DataFrames to PostgreSQL
def write_to_postgres(df, table_name):
    if df.count() == 0:
        print(f"No data found for table: {table_name}. Skipping...")
        return
    df.write \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=write_properties)
    print(f"Data written successfully to table: {table_name}")

patients_df = fhir_df.select(F.explode(col("entry")).alias("entry")) \
    .select("entry.resource.*") \
    .filter(col("resourceType") == "Patient") \
    .select(
        col("id").alias("patient_id"),
        col("name")[0]["given"].getItem(0).alias("first_name"),
        col("name")[0]["family"].alias("last_name"),
        col("gender"),
        col("birthDate").alias("birth_date"),
        # Extracting birth place
        F.expr("filter(extension, x -> x.url = 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace')[0].valueAddress").alias("birth_place")
    )

# Now, let's split the birth place into city, state, and country
# Assuming the birth place is in the format {City, Country, State}
# Now, let's extract the city, state, and country directly from the struct
patients_df = patients_df.withColumn("birth_place_city", F.col("birth_place.city")) \
    .withColumn("birth_place_state", F.col("birth_place.state")) \
    .withColumn("birth_place_country", F.col("birth_place.country"))


# Drop the original birth_place column if not needed
patients_df = patients_df.drop("birth_place")
#write_to_postgres(patients_df, "patients")

# Check the structure of the total field in the Claim resource
fhir_df.select(F.explode(col("entry")).alias("entry")) \
    .select("entry.resource.total") \
    .filter(col("resourceType") == "Claim") \
    .show(truncate=False)
# Extracting claims from the FHIR DataFrame
claims_df = fhir_df.select(F.explode(col("entry")).alias("entry")) \
    .select("entry.resource.*") \
    .filter(col("resourceType") == "Claim") \
    .select(
        col("id").alias("claim_id"),
        col("patient.reference").alias("patient_id"),
        col("billablePeriod.start").alias("billable_start"),
        col("billablePeriod.end").alias("billable_end"),
        F.when(col("total").isNotNull(), col("total.value").cast("double")).otherwise(None).alias("total_amount"),
        col("provider.display").alias("provider")
    )

# Print the schema to verify
patients_df.printSchema()