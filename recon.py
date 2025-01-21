from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when,size,expr,count,lit,coalesce
import os
from dotenv import load_dotenv
from pyspark.sql.types import DateType, TimestampType

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


# Initialize Spark session
spark = SparkSession.builder \
    .appName("FHIR JSON to PostgreSQL") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

json_file_path = "data/Bette450_Anderson154_6e4ac285-2a8d-a30d-5ecb-e32cb595a876.json"

# Read the JSON file into a DataFrame
if not os.path.exists(json_file_path):
    print(f"File not found: {json_file_path}")
    spark.stop()
    exit()

df = spark.read.json(json_file_path, multiLine=True)

# Explode the 'entry' array to get the nested structures for each resource
df_exploded = df.withColumn("entry", explode(col("entry")))
print("Exploded 'entry' array.")

# Dictionary to store processed DataFrames for each resource type
processed_data = {}

def write_to_postgres(df_resource, resource_type):
    try:
        jdbc_url = "jdbc:postgresql://localhost:5432/fhir_data"
        write_properties = {
            "user": "aarthi",
            "password": "aarthi",
            "driver": "org.postgresql.Driver"
        }
        df_resource.write.jdbc(url=jdbc_url, table=resource_type.lower(), mode="append", properties=write_properties)
        print(f"Data for {resource_type} written to PostgreSQL.")
        return True
    except Exception as e:
        print(f"Failed to write data for {resource_type} to PostgreSQL. Error: {e}")
        return False


# List of resource types to process
resource_types = ["Patient", "Encounter", "Condition", "DiagnosticReport", "Claim","DocumentReference","ExplanationOfBenefit","MedicationRequest","CareTeam","CarePlan", "Procedure", "Immunization"]

# Loop through each resource type and process data
for resource_type in resource_types:
    print(f"Processing {resource_type}...")

    if resource_type == "Patient":
        
        df_patient_only = df_exploded.filter(col("entry.resource.resourceType") == "Patient").select( col("entry.resource.id").alias("patient_id"), col("entry.resource.birthDate").alias("birth_date"), col("entry.resource.gender").alias("gender"), col("entry.resource.name")[0].getField("family").alias("family_name"), col("entry.resource.name")[0].getField("given")[0].alias("given_name"), col("entry.resource.address")[0].getField("city").alias("address_city"), col("entry.resource.address")[0].getField("state").alias("address_state"), col("entry.resource.address")[0].getField("country").alias("address_country"), col("entry.resource.maritalStatus.text").alias("marital_status"), col("entry.resource.telecom")[0].getField("value").alias("phone_number"), col("entry.resource.communication")[0].getField("language").getField("coding")[0].getField("display").alias("preferred_language") )
        # Extracting extensions
        df_extension = df_exploded.select(col("entry.resource.extension").alias("extensions"))
        df_race_ethnicity = df_extension.select(explode(col("extensions")).alias("extension"))
        df_race = df_race_ethnicity.filter(col("extension.url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race") \
                    .select(col("extension.extension").alias("race_extension"))
        df_ethnicity = df_race_ethnicity.filter(col("extension.url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity") \
                    .select(col("extension.extension").alias("ethnicity_extension"))
        # Extract race value
        df_race_value = df_race.select(explode(col("race_extension")).alias("race_sub_extension")) \
                    .filter(col("race_sub_extension.url") == "ombCategory") \
                    .select(col("race_sub_extension.valueCoding.display").alias("race"))
        # Extract ethnicity value
        df_ethnicity_value = df_ethnicity.select(explode(col("ethnicity_extension")).alias("ethnicity_sub_extension")) \
                    .filter(col("ethnicity_sub_extension.url") == "ombCategory") \
                    .select(col("ethnicity_sub_extension.valueCoding.display").alias("ethnicity"))
        df_resource = df_patient_only.join(df_race_value, how="left").join(df_ethnicity_value, how="left")
        df_resource = df_resource.withColumn("birth_date", col("birth_date").cast(DateType())) 


    elif resource_type == "Encounter":
        df_encounter = df_exploded.filter(col("entry.resource.resourceType") == "Encounter").select(
                    col("entry.resource.id").alias("encounter_id"),
                    col("entry.resource.subject.reference").alias("encounter_reference"),
                    col("entry.resource.status").alias("encounter_status"),
                    col("entry.resource.class.code").alias("class_code"),
                    col("entry.resource.class.system").alias("class_system"),
                    col("entry.resource.type").alias("type_text"), 
                    col("entry.resource.subject.display").alias("subject_display"),
                    col("entry.resource.period.start").alias("period_start"),
                    col("entry.resource.period.end").alias("period_end"),
                    col("entry.resource.serviceProvider.reference").alias("service_provider_reference"),
                    col("entry.resource.serviceProvider.display").alias("service_provider_display"),
        )
        # Exploding participants to get individual details
        df_participant = df_exploded.filter(col("entry.resource.resourceType") == "Encounter").select(
                    explode(col("entry.resource.participant")).alias("participant")
        )
        # Extracting participant details
        df_participant_details = df_participant.select(
                    col("participant.type").getItem(0).getField("text").alias("participant_type"),
                    col("participant.individual.reference").alias("individual_reference"),
                    col("participant.individual.display").alias("individual_display"),
                    col("participant.period.start").alias("participant_period_start"),
                    col("participant.period.end").alias("participant_period_end")
        )
        # Joining encounter data with participant details
        df_resource = df_encounter.join(df_participant_details, how="left")
        df_resource = df_resource.withColumn("period_start", col("period_start").cast(TimestampType())) \
                    .withColumn("period_end", col("period_end").cast(TimestampType())) \
                    .withColumn("participant_period_start", col("participant_period_start").cast(TimestampType())) \
                    .withColumn("participant_period_end", col("participant_period_end").cast(TimestampType()))

    elif resource_type == "Condition":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "Condition").select(
                    col("entry.resource.id").alias("condition_id"),
                    col("entry.resource.subject.reference").alias("patient_reference"),
                    col("entry.resource.clinicalStatus.coding").getItem(0).getField("code").alias("clinical_status_code"),
                    col("entry.resource.clinicalStatus.coding").getItem(0).getField("system").alias("clinical_status_system"),
                    col("entry.resource.verificationStatus.coding").getItem(0).getField("code").alias("verification_status_code"),
                    col("entry.resource.verificationStatus.coding").getItem(0).getField("system").alias("verification_status_system"),
                    col("entry.resource.category")[0].getField("coding").getItem(0).getField("code").alias("category_code"),
                    col("entry.resource.category")[0].getField("coding").getItem(0).getField("display").alias("category_display"),
                    col("entry.resource.code.coding").getItem(0).getField("code").alias("condition_code"),
                    col("entry.resource.code.coding").getItem(0).getField("display").alias("condition_display"),
                    col("entry.resource.code.text").alias("condition_text"),
                    col("entry.resource.encounter.reference").alias("encounter_reference"),
                    col("entry.resource.onsetDateTime").alias("onset_datetime"),
                    col("entry.resource.recordedDate").alias("recorded_datetime")
        )
    elif resource_type == "DiagnosticReport":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "DiagnosticReport").select(
                    col("entry.resource.id").alias("diagnostic_report_id"),
                    col("entry.resource.status").alias("status"),
                    col("entry.resource.effectiveDateTime").alias("effective_datetime"),
                    col("entry.resource.issued").alias("issued_datetime"),
                    col("entry.resource.subject.reference").alias("patient_reference"),
                    col("entry.resource.performer").getItem(0).getField("reference").alias("performer_reference"),
                    col("entry.resource.performer").getItem(0).getField("display").alias("performer_display"),
                    col("entry.resource.category").getItem(0).getField("coding").getItem(0).getField("code").alias("category_code"),
                    col("entry.resource.category").getItem(0).getField("coding").getItem(0).getField("display").alias("category_display"),
                    col("entry.resource.code").getField("coding").getItem(0).getField("code").alias("code"),
                    col("entry.resource.code").getField("coding").getItem(0).getField("display").alias("display"),
                    col("entry.resource.presentedForm").getItem(0).getField("contentType").alias("content_type"),
                    col("entry.resource.presentedForm").getItem(0).getField("data").alias("data")
        )
    elif resource_type == "Claim":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "Claim").select( col("entry.resource.id").alias("claim_id"), col("entry.resource.status").alias("status"), col("entry.resource.type").alias("type_text"), col("entry.resource.use").alias("use"), col("entry.resource.patient.reference").alias("patient_reference"), col("entry.resource.patient.display").alias("patient_display"), col("entry.resource.billablePeriod.start").alias("billable_period_start"), col("entry.resource.billablePeriod.end").alias("billable_period_end"), col("entry.resource.created").alias("created"), col("entry.resource.provider.reference").alias("provider_reference"), col("entry.resource.provider.display").alias("provider_display"), col("entry.resource.priority.coding")[0].getField("code").alias("priority_code"), col("entry.resource.facility.reference").alias("facility_reference"), col("entry.resource.facility.display").alias("facility_display"), col("entry.resource.diagnosis")[0].getField("sequence").alias("diagnosis_sequence_1"), col("entry.resource.diagnosis")[0].getField("diagnosisReference").getField("reference").alias("diagnosis_reference_1"), col("entry.resource.diagnosis")[1].getField("sequence").alias("diagnosis_sequence_2"), col("entry.resource.diagnosis")[1].getField("diagnosisReference").getField("reference").alias("diagnosis_reference_2"), col("entry.resource.insurance")[0].getField("coverage").getField("display").alias("insurance_coverage_display"), col("entry.resource.item")[0].getField("sequence").alias("item_sequence_1"), col("entry.resource.item")[0].getField("productOrService").getField("coding")[0].getField("display").alias("product_or_service_display_1"), col("entry.resource.item")[0].getField("encounter")[0].getField("reference").alias("encounter_reference_1"), col("entry.resource.item")[1].getField("sequence").alias("item_sequence_2"), col("entry.resource.item")[1].getField("diagnosisSequence")[0].alias("item_diagnosis_sequence_2"), col("entry.resource.item")[1].getField("productOrService").getField("coding")[0].getField("display").alias("product_or_service_display_2"), col("entry.resource.item")[2].getField("sequence").alias("item_sequence_3"), col("entry.resource.item")[2].getField("diagnosisSequence")[0].alias("item_diagnosis_sequence_3"), col("entry.resource.item")[2].getField("productOrService").getField("coding")[0].getField("display").alias("product_or_service_display_3"), 
                                                                                    col("entry.resource.total").alias("total"))
    
    elif resource_type == "DocumentReference":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "DocumentReference").select( col("entry.resource.id").alias("document_id"), col("entry.resource.meta.profile").alias("profile"), col("entry.resource.identifier")[0].getField("value").alias("identifier_value"), col("entry.resource.status").alias("status"), col("entry.resource.type").alias("type_text"), col("entry.resource.category")[0].getField("coding")[0].getField("display").alias("category_display"), col("entry.resource.subject.reference").alias("subject_reference"), col("entry.resource.date").alias("date"), col("entry.resource.author")[0].getField("display").alias("author_display"), col("entry.resource.custodian.display").alias("custodian_display"), col("entry.resource.content")[0].getField("attachment").getField("data").alias("content_data"), col("entry.resource.content")[0].getField("format").getField("display").alias("format_display"), col("entry.resource.context").getField("encounter")[0].getField("reference").alias("context_encounter_reference"), col("entry.resource.context").getField("period").getField("start").alias("context_period_start"), col("entry.resource.context").getField("period").getField("end").alias("context_period_end") ) 
    
    elif resource_type == "ExplanationOfBenefit":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "ExplanationOfBenefit").select(
                    col("entry.resource.id").alias("eob_id"),
                    col("entry.resource.status").alias("status"),
                    col("entry.resource.type").alias("claim_type"),
                    col("entry.resource.patient.reference").alias("patient_reference"),
                    col("entry.resource.billablePeriod.start").alias("billable_period_start"),
                    col("entry.resource.billablePeriod.end").alias("billable_period_end"),
                    col("entry.resource.payment.amount.value").alias("payment_value"),
                    col("entry.resource.payment.amount.currency").alias("payment_currency"),
                    explode(col("entry.resource.item")).alias("item")
                    ).select(
                    "eob_id",
                    "status",
                    "claim_type",
                    "patient_reference",
                    "billable_period_start",
                    "billable_period_end",
                    "payment_value",
                    "payment_currency",
                    col("item.sequence").alias("item_sequence"),
                    col("item.productOrService.coding")[0]["code"].alias("service_code"),
                    col("item.productOrService.coding")[0]["display"].alias("service_display"),
                    col("item.servicedPeriod.start").alias("service_start"),
                    col("item.servicedPeriod.end").alias("service_end"),
                    col("item.locationCodeableConcept.coding")[0]["display"].alias("service_location")
                    )                
        
    elif resource_type == "MedicationRequest":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "MedicationRequest").select(
                    col("entry.resource.id").alias("medication_request_id"),
                    col("entry.resource.meta.profile")[0].alias("profile"),
                    col("entry.resource.status").alias("status"),
                    col("entry.resource.intent").alias("intent"),
                    col("entry.resource.medicationCodeableConcept.coding")[0].getField("code").alias("medication_code"),
                    col("entry.resource.medicationCodeableConcept.coding")[0].getField("display").alias("medication_display"),
                    col("entry.resource.medicationCodeableConcept.text").alias("medication_text"),
                    col("entry.resource.subject.reference").alias("subject_reference"),
                    col("entry.resource.encounter.reference").alias("encounter_reference"),
                    col("entry.resource.authoredOn").alias("authored_on"),
                    col("entry.resource.requester.reference").alias("requester_reference"),
                    col("entry.resource.requester.display").alias("requester_display"),
                    col("entry.resource.reasonReference")[0].getField("reference").alias("reason_reference"),
                    col("entry.resource.dosageInstruction")[0].getField("sequence").alias("dosage_sequence"),
                    #col("entry.resource.dosageInstruction")[0].getField("text").alias("dosage_text"),
                    col("entry.resource.dosageInstruction")[0].getField("asNeededBoolean").alias("as_needed_boolean")
                    )
        
    elif resource_type == "CareTeam":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "CareTeam").select( col("entry.resource.id").alias("care_team_id"), col("entry.resource.meta.profile")[0].alias("profile"), col("entry.resource.status").alias("status"), col("entry.resource.subject.reference").alias("subject_reference"), col("entry.resource.encounter.reference").alias("encounter_reference"), col("entry.resource.period.start").alias("period_start"), col("entry.resource.period.end").alias("period_end"), col("entry.resource.reasonCode")[0].getField("coding")[0].getField("display").alias("reason_code_display"), col("entry.resource.reasonCode")[0].getField("text").alias("reason_code_text"), col("entry.resource.managingOrganization")[0].getField("reference").alias("managing_organization_reference"), col("entry.resource.managingOrganization")[0].getField("display").alias("managing_organization_display") ) # Exploding the participant array to extract nested fields df_care_team = df_care_team.withColumn("participant", explode(col("entry.resource.participant"))) # Selecting additional participant fields after explosion df_care_team = df_care_team.select( col("*"), col("participant.role")[0].getField("coding")[0].getField("display").alias("participant_role_display"), col("participant.role")[0].getField("text").alias("participant_role_text"), col("participant.member.reference").alias("participant_member_reference"), col("participant.member.display").alias("participant_member_display") ) 

    elif resource_type == "CarePlan":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "CarePlan").select( col("entry.resource.id").alias("care_plan_id"), col("entry.resource.meta.profile")[0].alias("profile"), col("entry.resource.text.status").alias("text_status"), col("entry.resource.text.div").alias("text_div"), col("entry.resource.status").alias("status"), col("entry.resource.intent").alias("intent"), col("entry.resource.category")[0].getField("coding")[0].getField("system").alias("category_system_1"), col("entry.resource.category")[0].getField("coding")[0].getField("code").alias("category_code_1"), col("entry.resource.category")[1].getField("coding")[0].getField("system").alias("category_system_2"), col("entry.resource.category")[1].getField("coding")[0].getField("code").alias("category_code_2"), col("entry.resource.category")[1].getField("coding")[0].getField("display").alias("category_display_2"), col("entry.resource.category")[1].getField("text").alias("category_text_2"), col("entry.resource.subject.reference").alias("subject_reference"), col("entry.resource.encounter.reference").alias("encounter_reference"), col("entry.resource.period.start").alias("period_start"), col("entry.resource.careTeam")[0].getField("reference").alias("care_team_reference"), col("entry.resource.addresses")[0].getField("reference").alias("addresses_reference"), col("entry.resource.activity")[0].getField("detail").getField("code").getField("coding")[0].getField("system").alias("activity_code_system_1"), col("entry.resource.activity")[0].getField("detail").getField("code").getField("coding")[0].getField("code").alias("activity_code_code_1"), col("entry.resource.activity")[0].getField("detail").getField("code").getField("coding")[0].getField("display").alias("activity_code_display_1"), col("entry.resource.activity")[0].getField("detail").getField("code").getField("text").alias("activity_code_text_1"), col("entry.resource.activity")[0].getField("detail").getField("status").alias("activity_status_1"), col("entry.resource.activity")[0].getField("detail").getField("location").getField("display").alias("activity_location_display_1"), col("entry.resource.activity")[1].getField("detail").getField("code").getField("coding")[0].getField("system").alias("activity_code_system_2"), col("entry.resource.activity")[1].getField("detail").getField("code").getField("coding")[0].getField("code").alias("activity_code_code_2"), col("entry.resource.activity")[1].getField("detail").getField("code").getField("coding")[0].getField("display").alias("activity_code_display_2"), col("entry.resource.activity")[1].getField("detail").getField("code").getField("text").alias("activity_code_text_2"), col("entry.resource.activity")[1].getField("detail").getField("status").alias("activity_status_2"), col("entry.resource.activity")[1].getField("detail").getField("location").getField("display").alias("activity_location_display_2") ) 

    elif resource_type == "Procedure":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "Procedure").select( col("entry.resource.id").alias("procedure_id"), col("entry.resource.meta.profile")[0].alias("profile"), col("entry.resource.status").alias("status"), col("entry.resource.code.coding")[0].getField("system").alias("code_system"), col("entry.resource.code.coding")[0].getField("code").alias("code"), col("entry.resource.code.coding")[0].getField("display").alias("code_display"), col("entry.resource.code.text").alias("code_text"), col("entry.resource.subject.reference").alias("subject_reference"), col("entry.resource.encounter.reference").alias("encounter_reference"), col("entry.resource.performedPeriod.start").alias("performed_period_start"), col("entry.resource.performedPeriod.end").alias("performed_period_end"), col("entry.resource.location").alias("location") ) 

    elif resource_type == "Immunization":
        df_resource = df_exploded.filter(col("entry.resource.resourceType") == "Immunization").select( col("entry.resource.id").alias("immunization_id"),col("entry.resource.subject.reference").alias("immunization_reference"), col("entry.resource.meta.profile")[0].alias("profile"), col("entry.resource.status").alias("status"), col("entry.resource.vaccineCode.coding")[0].getField("system").alias("vaccine_code_system"), col("entry.resource.vaccineCode.coding")[0].getField("code").alias("vaccine_code"), col("entry.resource.vaccineCode.coding")[0].getField("display").alias("vaccine_code_display"), col("entry.resource.vaccineCode.text").alias("vaccine_text"), col("entry.resource.patient.reference").alias("patient_reference"), col("entry.resource.encounter.reference").alias("encounter_reference"), col("entry.resource.occurrenceDateTime").alias("occurrence_date_time"), col("entry.resource.primarySource").alias("primary_source"), col("entry.resource.location").alias("location") ) 

    # Print schema and a few rows for debugging
    print(f"Schema for {resource_type}:")
    df_resource.printSchema()
    print(f"Data for {resource_type}:")
    df_resource.show(5)

    # Write DataFrame to PostgreSQL and stop if writing fails
    if not write_to_postgres(df_resource, resource_type):
        print(f"Stopping processing due to failure in writing {resource_type} data.")
        break
    
    # Store the processed DataFrame
    processed_data[resource_type] = df_resource

print("Data processing complete.")

# Reconciliation Process
def perform_reconciliation(input_df, resource_type):
    print(f"Starting reconciliation for {resource_type}...")
    
    # Load data from PostgreSQL table for reconciliation
    jdbc_url = "jdbc:postgresql://localhost:5432/fhir_data"
    write_properties = {
        "user": "aarthi",
        "password": "aarthi",
        "driver": "org.postgresql.Driver"
    }
    df_postgres = spark.read.jdbc(url=jdbc_url, table=resource_type.lower(), properties=write_properties)

    # Determine the key column for reconciliation
    key_column = None
    if resource_type == "Patient":
        key_column = "patient_id"
    elif resource_type == "Encounter":
        key_column = "encounter_id"
    elif resource_type == "Condition":
        key_column = "condition_id"
    elif resource_type == "DiagnosticReport":
        key_column = "diagnostic_report_id"
    elif resource_type == "Claim":
        key_column = "claim_id"
    elif resource_type == "DocumentReference":
        key_column = "document_id"
    elif resource_type == "ExplanationOfBenefit":
        key_column = "eob_id"
    elif resource_type == "MedicationRequest":
        key_column = "medication_request_id"
    elif resource_type == "CareTeam":
        key_column = "care_team_id"
    elif resource_type == "CarePlan":
        key_column = "care_plan_id"
    elif resource_type == "Procedure":
        key_column = "procedure_id"
    elif resource_type == "Immunization":
        key_column = "immunization_id"
    else:
        print(f"Unknown resource type: {resource_type}") 
        return
    # Ensure patient_id is captured correctly
    df_postgres = df_postgres.withColumnRenamed(key_column, f"postgres_{key_column}")
    input_df = input_df.withColumnRenamed(key_column, f"input_{key_column}")

    # Reconciliation Process
    recon_result = input_df.join(df_postgres, input_df[f"input_{key_column}"] == df_postgres[f"postgres_{key_column}"], "outer") \
        .select(coalesce(input_df[f"input_{key_column}"], df_postgres[f"postgres_{key_column}"]).alias("patient_id"), \
                input_df[f"input_{key_column}"], df_postgres[f"postgres_{key_column}"]) \
        .withColumn("status", 
                    when(col(f"input_{key_column}").isNull(), "Missing in Input")
                    .when(col(f"postgres_{key_column}").isNull(), "Missing in PostgreSQL")
                    .otherwise("Match")) \

    # Generate reconciliation report
    recon_report = recon_result.groupBy("status").agg(count("*").alias("count"))
    recon_report.show()

    # Handle discrepancies by writing to Reconciliation table
    discrepancies = recon_result.filter(col("status") != "Match") \
                             .select(col("patient_id"), col("status"))

    
    discrepancies.show()  # Debugging

    discrepancies.write.mode("append").jdbc(url=jdbc_url, table="reconciliation", properties=write_properties)
    print(f"Reconciliation for {resource_type} complete.")

# Perform reconciliation for each resource type
for resource_type, df in processed_data.items():
    perform_reconciliation(df, resource_type)

print("Reconciliation complete.")
    

# Stop Spark session
spark.stop()
