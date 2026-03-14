import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_credit_applications",
    comment="Cleaned credit applications dataset"
)
def silver_credit_applications():

    df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_credit_applications") \
              .withColumnRenamed("application_id", "applicant_id")

    # Remove duplicate applications
    df = df.dropDuplicates(["applicant_id"])

    # Cast numeric columns
    df = df.withColumn("loan_amount", col("loan_amount").cast("double")) \
           .withColumn("rate_of_interest", col("rate_of_interest").cast("double")) \
           .withColumn("interest_rate_spread", col("interest_rate_spread").cast("double")) \
           .withColumn("upfront_charges", col("upfront_charges").cast("double")) \
           .withColumn("term", col("term").cast("int")) \
           .withColumn("application_status", col("application_status").cast("int")) \
           .withColumn("year", col("year").cast("int"))

    # Replace negative numeric values with NULL
    df = df.withColumn(
        "rate_of_interest",
        when(col("rate_of_interest") < 0, None).otherwise(col("rate_of_interest"))
    ).withColumn(
        "interest_rate_spread",
        when(col("interest_rate_spread") < 0, None).otherwise(col("interest_rate_spread"))
    ).withColumn(
        "upfront_charges",
        when(col("upfront_charges") < 0, None).otherwise(col("upfront_charges"))
    )

    # Calculate medians
    median_rate = df.selectExpr("percentile_approx(rate_of_interest,0.5)").first()[0]
    median_spread = df.selectExpr("percentile_approx(interest_rate_spread,0.5)").first()[0]
    median_upfront = df.selectExpr("percentile_approx(upfront_charges,0.5)").first()[0]
    median_loan = df.selectExpr("percentile_approx(loan_amount,0.5)").first()[0]
    median_term = df.selectExpr("percentile_approx(term,0.5)").first()[0]

    # Replace NULL numeric values
    df = df.withColumn(
        "rate_of_interest",
        coalesce(col("rate_of_interest"), lit(median_rate))
    )

    df = df.withColumn(
        "interest_rate_spread",
        coalesce(col("interest_rate_spread"), lit(median_spread))
    )

    df = df.withColumn(
        "upfront_charges",
        coalesce(col("upfront_charges"), lit(median_upfront))
    )

    df = df.withColumn(
        "loan_amount",
        coalesce(col("loan_amount"), lit(median_loan))
    )

    df = df.withColumn(
        "term",
        coalesce(col("term"), lit(median_term))
    )

    # Fix missing applicant_id
    df = df.filter(col("applicant_id").isNotNull())

    # Fix missing year
    df = df.fillna({"year": 2019})

    # Fix missing application_status
    df = df.fillna({"application_status": 0})

    # Standardize categorical columns
    df = df.withColumn("loan_type", upper(trim(col("loan_type")))) \
           .withColumn("loan_purpose", upper(trim(col("loan_purpose")))) \
           .withColumn("submission_of_application", upper(trim(col("submission_of_application")))) \
           .withColumn("region", upper(trim(col("region")))) \
           .withColumn("approv_in_adv", upper(trim(col("approv_in_adv")))) \
           .withColumn("loan_limit", upper(trim(col("loan_limit"))))

    # Fix NULL and blank values
    df = df.withColumn(
        "loan_limit",
        when((col("loan_limit").isNull()) | (trim(col("loan_limit")) == ""), "UNKNOWN")
        .otherwise(col("loan_limit"))
    )

    df = df.withColumn(
        "approv_in_adv",
        when((col("approv_in_adv").isNull()) | (trim(col("approv_in_adv")) == ""), "UNKNOWN")
        .otherwise(col("approv_in_adv"))
    )

    # Data quality flag
    df = df.withColumn(
        "data_quality_flag",
        when(col("rate_of_interest") <= 0, "INVALID_RATE")
        .otherwise("VALID")
    )

    # Processing timestamp
    df = df.withColumn("processing_timestamp", current_timestamp())

    required_cols = [
        "Neg_ammortization",
        "interest_only",
        "lump_sum_payment",
        "business_or_commercial",
        "submission_of_application"
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit("UNKNOWN"))
    return df