import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_economic_indicators",
    comment="Cleaned economic indicators dataset joined with credit applications"
)
def silver_economic_indicators():

    # Read economic data
    econ = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_economic_indicators")

    # Read credit applications (only required columns)
    apps = dlt.read("silver_credit_applications").select("applicant_id", "region")

    # Standardize region
    econ = econ.withColumn("region", upper(col("region")))
    apps = apps.withColumn("region", upper(col("region")))

    # LEFT JOIN (avoids duplicate region columns)
    df = econ.join(apps, on="region", how="left")

    # Cast numeric columns
    df = df.withColumn("avg_property_value", col("avg_property_value").cast("double")) \
           .withColumn("avg_interest_rate", col("avg_interest_rate").cast("double")) \
           .withColumn("interest_rate_spread", col("interest_rate_spread").cast("double"))

    # Handle invalid values
    df = df.withColumn(
        "avg_property_value",
        when(col("avg_property_value") <= 0, None).otherwise(col("avg_property_value"))
    ).withColumn(
        "avg_interest_rate",
        when(col("avg_interest_rate") <= 0, None).otherwise(col("avg_interest_rate"))
    ).withColumn(
        "interest_rate_spread",
        when(col("interest_rate_spread") < 0, None).otherwise(col("interest_rate_spread"))
    )

    # Interest rate category
    df = df.withColumn(
        "interest_rate_category",
        when(col("avg_interest_rate") < 3, "LOW_RATE")
        .when(col("avg_interest_rate") < 6, "MEDIUM_RATE")
        .otherwise("HIGH_RATE")
    )

    # Add timestamp
    df = df.withColumn("processing_timestamp", current_timestamp())

    return df