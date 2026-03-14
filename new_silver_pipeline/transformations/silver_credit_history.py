import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_credit_history",
    comment="Cleaned credit history dataset"
)
def silver_credit_history():

    df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_credit_history")

    # Remove duplicates
    df = df.dropDuplicates(["applicant_id"])

    # Cast numeric columns
    df = df.withColumn("credit_score", col("credit_score").cast("int")) \
           .withColumn("debt_to_income_ratio", col("debt_to_income_ratio").cast("double"))

    # Standardize categorical columns
    df = df.withColumn("credit_worthiness", upper(trim(col("credit_worthiness")))) \
           .withColumn("open_credit", upper(trim(col("open_credit")))) \
           .withColumn("credit_type", upper(trim(col("credit_type")))) \
           .withColumn("co_applicant_credit_type", upper(trim(col("co_applicant_credit_type")))) \
           .withColumn("negative_amortization", upper(trim(col("negative_amortization")))) \
           .withColumn("interest_only", upper(trim(col("interest_only")))) \
           .withColumn("lump_sum_payment", upper(trim(col("lump_sum_payment"))))

    # Replace NULL or blank open_credit values
    df = df.withColumn(
        "open_credit",
        when((col("open_credit").isNull()) | (trim(col("open_credit")) == ""), "UNKNOWN")
        .otherwise(col("open_credit"))
    )

    # Validate credit score range
    df = df.withColumn(
        "credit_score",
        when((col("credit_score") < 300) | (col("credit_score") > 850), None)
        .otherwise(col("credit_score"))
    )

    # Calculate median credit score
    median_score = df.selectExpr("percentile_approx(credit_score,0.5)").first()[0]

    # Replace NULL credit scores
    df = df.withColumn(
        "credit_score",
        coalesce(col("credit_score"), lit(median_score))
    )

    # Replace NULL numeric values
    df = df.fillna({
        "debt_to_income_ratio": 0
    })

    # Credit score band
    df = df.withColumn(
        "credit_score_band",
        when(col("credit_score") < 580, "POOR")
        .when(col("credit_score") <= 669, "FAIR")
        .when(col("credit_score") <= 739, "GOOD")
        .when(col("credit_score") <= 799, "VERY_GOOD")
        .otherwise("EXCELLENT")
    )

    # Risk category
    df = df.withColumn(
        "risk_category",
        when((col("credit_score") < 600) | (col("debt_to_income_ratio") > 40), "HIGH_RISK")
        .when(col("credit_score") <= 700, "MEDIUM_RISK")
        .otherwise("LOW_RISK")
    )

    # Processing timestamp
    df = df.withColumn("processing_timestamp", current_timestamp())

    return df