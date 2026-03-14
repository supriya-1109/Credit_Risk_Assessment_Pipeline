import dlt
from pyspark.sql.functions import *

# =====================================================
# DIM_BORROWER
# =====================================================

@dlt.table(
    name="dim_borrower",
    comment="Borrower Dimension Table"
)
def dim_borrower():

    ap = spark.table("credits_catalog.silver_credit_risk_schema.silver_applicant_profiles")

    return ap.select(
        col("applicant_id").alias("borrower_key"),
        col("gender"),
        col("age"),
        col("income")
    ).dropDuplicates()


# =====================================================
# DIM_CREDIT
# =====================================================

@dlt.table(
    name="dim_credit",
    comment="Credit Dimension Table"
)
def dim_credit():

    ch = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_history")

    return ch.select(
        col("applicant_id").alias("credit_key"),
        col("credit_score").alias("Credit_Score"),
        col("credit_type"),
        col("Credit_Worthiness"),
        col("co_applicant_credit_type"),
        col("open_credit")
    ).dropDuplicates()


# =====================================================
# DIM_PROPERTY
# =====================================================

@dlt.table(
    name="dim_property",
    comment="Property Dimension Table"
)
def dim_property():

    ld = spark.table("credits_catalog.silver_credit_risk_schema.silver_loan_details")

    return ld.select(
        monotonically_increasing_id().alias("property_key"),
        col("property_value"),
        col("construction_type"),
        col("occupancy_type"),
        col("secured_by"),
        col("total_units"),
        col("security_type")
    ).dropDuplicates()
# =====================================================
# DIM_LOAN
# =====================================================

@dlt.table(
    name="dim_loan",
    comment="Loan Dimension Table"
)
def dim_loan():

    ca = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_applications")

    return ca.select(
        monotonically_increasing_id().alias("loan_key"),
        col("loan_type"),
        col("loan_purpose"),
        col("loan_limit"),
        col("approv_in_adv"),
        col("term"),
        col("Neg_ammortization"),
        col("interest_only"),
        col("lump_sum_payment"),
        col("business_or_commercial"),
        col("submission_of_application")
    ).dropDuplicates()
# =====================================================
# DIM_TIME
# =====================================================

@dlt.table(
    name="dim_time",
    comment="Time Dimension Table"
)
def dim_time():

    ca = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_applications")

    return ca.select(
        monotonically_increasing_id().alias("time_key"),
        col("year")
    ).dropDuplicates()


# =====================================================
# DIM_LOCATION
# =====================================================

@dlt.table(
    name="dim_location",
    comment="Location Dimension Table"
)
def dim_location():

    ap = spark.table("credits_catalog.silver_credit_risk_schema.silver_applicant_profiles")

    return ap.select(
        monotonically_increasing_id().alias("region_key"),
        col("region").alias("Region")
    ).dropDuplicates()


# =====================================================
# FACT_LOAN_APPLICATION
# =====================================================
@dlt.table(
    name="fact_loan_application",
    comment="Loan Application Fact Table with Risk Metrics"
)
def fact_loan_application():

    ap = spark.table("credits_catalog.silver_credit_risk_schema.silver_applicant_profiles").alias("ap")
    ld = spark.table("credits_catalog.silver_credit_risk_schema.silver_loan_details").alias("ld")
    ch = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_history").alias("ch")
    ca = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_applications").alias("ca")

    df = ap.join(ld, col("ap.applicant_id") == col("ld.applicant_id"), "left") \
           .join(ch, col("ap.applicant_id") == col("ch.applicant_id"), "left") \
           .join(ca, col("ap.applicant_id") == col("ca.applicant_id"), "left")

    # -------------------------------------------------
    # Existing Metric
    # -------------------------------------------------
    df = df.withColumn(
        "LTV",
        when(col("ld.property_value")!=0,
            col("ld.loan_amount") / col("ld.property_value"))
    )

    # -------------------------------------------------
    # New Metrics
    # -------------------------------------------------

    # Loan to Income Ratio
    df = df.withColumn(
        "loan_to_income_ratio",
        when(col("ap.income")!=0,
            col("ld.loan_amount") / col("ap.income"))
    )

    # Previous Default Flag
    df = df.withColumn(
        "previous_default_flag",
        when(col("ca.status") == "Default", 1).otherwise(0)
    )

    # Risk Score
    df = df.withColumn(
        "risk_score",
        when(col("ch.credit_score") < 580, 0.9)
        .when(col("ch.credit_score") < 670, 0.7)
        .when(col("ch.credit_score") < 740, 0.5)
        .otherwise(0.2)
    )

    # Risk Category
    df = df.withColumn(
        "risk_category",
        when(col("risk_score") >= 0.8, "High")
        .when(col("risk_score") >= 0.5, "Medium")
        .otherwise("Low")
    )

    return df.select(
        monotonically_increasing_id().alias("loan_id"),
        col("ap.applicant_id").alias("borrower_key"),
        col("ld.loan_type").alias("loan_key"),
        col("ch.credit_score").alias("credit_key"),
        col("ld.property_value").alias("property_key"),
        col("ap.region").alias("region_key"),
        col("ca.year").alias("time_key"),
        col("ld.loan_amount"),
        col("ca.rate_of_interest"),
        col("ca.interest_rate_spread"),
        col("ld.property_value"),
        col("ap.income"),
        col("LTV"),
        col("loan_to_income_ratio"),
        col("ch.debt_to_income_ratio").alias("dtir1"),
        col("previous_default_flag"),
        col("risk_score"),
        col("risk_category"),
        col("ca.status")
    )