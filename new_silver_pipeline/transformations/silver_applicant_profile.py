import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_applicant_profiles",
    comment="Cleaned applicant profile dataset"
)
def silver_applicant_profiles():

    df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_applicant_profiles")

    # Remove duplicates
    df = df.dropDuplicates(["applicant_id"])

    # Cast numeric columns
    df = df.withColumn("income", col("income").cast("double"))

    # Handle missing income
    df = df.fillna({
        "income": 0
    })

    # Standardize gender and replace 'Sex Not Available'
    df = df.withColumn(
        "gender",
        when(col("gender") == "Sex Not Available", "UNKNOWN")
        .otherwise(upper(trim(col("gender"))))
    )

    # Standardize categorical columns
    df = df.withColumn("age", upper(trim(col("age")))) \
           .withColumn("region", upper(trim(col("region")))) \
           .withColumn("business_or_commercial", upper(trim(col("business_or_commercial")))) \
           .withColumn("occupancy_type", upper(trim(col("occupancy_type")))) \
           .withColumn("construction_type", upper(trim(col("construction_type")))) \
           .withColumn("total_units", upper(trim(col("total_units"))))

    # Data quality flag
    df = df.withColumn(
        "data_quality_flag",
        when(col("income") <= 0, "INVALID_INCOME")
        .otherwise("VALID")
    )

    # Processing timestamp
    df = df.withColumn("processing_timestamp", current_timestamp())

    return df



