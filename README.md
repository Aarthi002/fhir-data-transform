FHIR Data Transformation and Reconciliation POC
Overview
This project is a Proof of Concept (POC) for transforming and reconciling FHIR (Fast Healthcare Interoperability Resources) JSON data into a PostgreSQL database using Apache Spark. It processes various FHIR resource types, writes the processed data into PostgreSQL, and performs reconciliation between the input JSON and database records.

Workflow
Environment Setup:
Loads environment variables from config/database.env for database credentials.
Uses the PostgreSQL JDBC driver for database connectivity.
Data Ingestion:
Reads a multi-line JSON file containing FHIR data.
Explodes the entry array to process nested FHIR resource structures.
Data Transformation:
Processes the following FHIR resource types:
Patient
Encounter
Condition
DiagnosticReport
Claim
DocumentReference
ExplanationOfBenefit
MedicationRequest
CareTeam
CarePlan
Procedure
Immunization
Extracts and transforms specific fields for each resource type, including nested fields and arrays.
Data Persistence:
Writes the transformed data to PostgreSQL, using table names based on resource types.
Reconciliation:
Compares the input JSON data with PostgreSQL records for each resource type.
Identifies discrepancies such as:
Records missing in the input JSON.
Records missing in PostgreSQL.
Matched records.
Generates a reconciliation report and stores discrepancies in a reconciliation table.

File Structure
recon.py: Main script for data processing and reconciliation.
config/database.env: Stores database connection details.
libs/postgresql-42.5.6.jar: PostgreSQL JDBC driver for Spark.
data/: Folder containing the input FHIR JSON file.

Key Components
Environment Variables
Environment variables are loaded from the .env file to ensure secure handling of sensitive data:
DB_USER: PostgreSQL username.
DB_PASSWORD: PostgreSQL password.
DB_HOST: Hostname for the PostgreSQL database.
DB_PORT: Port for the PostgreSQL database.
DB_NAME: Database name.
Spark Session
A Spark session is initialized with the required configurations:
App name: FHIR JSON to PostgreSQL.
JDBC driver configuration for PostgreSQL.
Data Processing
For each resource type:
Filter and extract relevant fields using Spark SQL functions.
Transform nested arrays and objects (e.g., extension, participant).
Cast fields to appropriate data types (DateType, TimestampType).
Data Writing
The transformed data is written to PostgreSQL:
Tables are named based on resource types (e.g., Patient, Encounter).
Reconciliation
Reads the corresponding table from PostgreSQL.
Joins input data with database records using the resource ID as the key.
Identifies and classifies discrepancies:
Missing in Input
Missing in PostgreSQL
Match
Writes discrepancies to a reconciliation table.

Setup Instructions
Prerequisites
Python 3.9 or higher.
Apache Spark.
PostgreSQL database.
PostgreSQL JDBC driver.
.env file with database credentials.
Steps
Clone the repository:
bash
Copy code
git clone <repository_url>
cd <repository_name>


Install Dependencies:
Install Python dependencies:
bash
Copy code
pip install pyspark python-dotenv


Place the JDBC driver in the libs/ directory.
Configure Environment Variables:
Update config/database.env with PostgreSQL credentials.
Run the Script:
bash
Copy code
spark-submit --jars libs/postgresql-42.5.6.jar recon.py



Output
Processed Data:
Transformed FHIR resource data written to PostgreSQL tables.
Reconciliation Report:
Summary of discrepancies stored in the reconciliation table.
Example statuses:
Match
Missing in Input
Missing in PostgreSQL

