import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_loan_details",
    comment="Cleaned loan details dataset"
)
def silver_loan_details():

    df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_loan_details")

    # Remove duplicates
    df = df.dropDuplicates(["applicant_id"])

    # Cast numeric columns
    df = df.withColumn("loan_amount", col("loan_amount").cast("double")) \
           .withColumn("loan_term", col("loan_term").cast("int")) \
           .withColumn("property_value", col("property_value").cast("double")) \
           .withColumn("loan_to_value", col("loan_to_value").cast("double"))

    # Validate loan_to_value
    df = df.withColumn(
        "loan_to_value",
        when((col("loan_to_value") < 0) | (col("loan_to_value") > 100), None)
        .otherwise(col("loan_to_value"))
    )

    # Validate loan term
    df = df.withColumn(
        "loan_term",
        when((col("loan_term") <= 0) | (col("loan_term") > 600), None)
        .otherwise(col("loan_term"))
    )

    # Calculate medians
    median_loan = df.selectExpr("percentile_approx(loan_amount,0.5)").first()[0]
    median_term = df.selectExpr("percentile_approx(loan_term,0.5)").first()[0]
    median_ltv = df.selectExpr("percentile_approx(loan_to_value,0.5)").first()[0]
    median_property = df.selectExpr("percentile_approx(property_value,0.5)").first()[0]

    # Fill NULL values
    df = df.withColumn("loan_amount", coalesce(col("loan_amount"), lit(median_loan))) \
           .withColumn("loan_term", coalesce(col("loan_term"), lit(median_term))) \
           .withColumn("loan_to_value", coalesce(col("loan_to_value"), lit(median_ltv))) \
           .withColumn("property_value", coalesce(col("property_value"), lit(median_property)))

    # Standardize categorical columns
    df = df.withColumn("loan_type", upper(trim(col("loan_type")))) \
           .withColumn("loan_purpose", upper(trim(col("loan_purpose"))))

    # Only transform if column exists
    if "interest_only" in df.columns:
        df = df.withColumn("interest_only", upper(trim(col("interest_only"))))

    if "neg_amortization" in df.columns:
        df = df.withColumn("neg_amortization", upper(trim(col("neg_amortization"))))

    # Loan size category
    df = df.withColumn(
        "loan_size_category",
        when(col("loan_amount") < 100000, "SMALL")
        .when(col("loan_amount") < 500000, "MEDIUM")
        .otherwise("LARGE")
    )

    # Data quality flag
    df = df.withColumn(
        "data_quality_flag",
        when(col("loan_amount") <= 0, "INVALID_LOAN")
        .otherwise("VALID")
    )

    # Processing timestamp
    df = df.withColumn("processing_timestamp", current_timestamp())

    if "construction_type" not in df.columns:
        df = df.withColumn("construction_type", lit("UNKNOWN"))

    if "occupancy_type" not in df.columns:
        df = df.withColumn("occupancy_type", lit("UNKNOWN"))
    if "secured_by" not in df.columns:
        df = df.withColumn("secured_by", lit("UNKNOWN"))

    if "total_units" not in df.columns:
        df = df.withColumn("total_units", lit("UNKNOWN"))
    if "security_type" not in df.columns:
        df = df.withColumn("security_type", lit("UNKNOWN"))

    if "property_value" not in df.columns:
        df = df.withColumn("property_value", lit(0.0).cast("double"))
    return df