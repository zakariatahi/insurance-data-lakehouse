# ============================================================
# SILVER -> GOLD
# ============================================================

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    avg,
    max,
    min,
    count,
    first,
    last
)


# ============================================================
# CONFIGURATION
# ============================================================

catalog = "smart_claims_catalog"

silver_schema = "02_silver"
gold_schema = "03_gold"


# ============================================================
# 1. AGGREGATED TELEMATICS
# ============================================================
#
# Source:
#   smart_claims_catalog.02_silver.telematics
#
# Creates one row per chassis_no.
#
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.aggregated_telematics",
    comment="Aggregated telematics information per vehicle",
    table_properties={
        "quality": "gold"
    }
)
def aggregated_telematics():

    telematics = spark.read.table(
        f"{catalog}.{silver_schema}.telematics"
    )

    return (
        telematics
        .groupBy("chassis_no")
        .agg(

            # Speed statistics
            avg("speed").alias("avg_speed"),
            max("speed").alias("max_speed"),
            min("speed").alias("min_speed"),

            # Number of telematics events
            count("*").alias("telematics_event_count"),

            # Last known coordinates
            last("latitude", ignorenulls=True).alias(
                "last_latitude"
            ),

            last("longitude", ignorenulls=True).alias(
                "last_longitude"
            ),

            # Most recent telematics timestamp
            max("event_timestamp").alias(
                "last_telematics_timestamp"
            )
        )
    )


# ============================================================
# 2. CUSTOMER + POLICY + CLAIM
# ============================================================
#
# customers
#     |
# customer_id
#     |
# policies
#     |
# policy_id
#     |
# claims
#
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.customer_claim_policy",
    comment="Combined customer, policy and claim information",
    table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy():

    # --------------------------------------------------------
    # READ SILVER TABLES
    # --------------------------------------------------------

    customers = spark.read.table(
        f"{catalog}.{silver_schema}.customers"
    )

    policies = spark.read.table(
        f"{catalog}.{silver_schema}.policies"
    )

    claims = spark.read.table(
        f"{catalog}.{silver_schema}.claims"
    )


    # --------------------------------------------------------
    # CLAIMS + POLICIES
    # --------------------------------------------------------

    claim_policy = (
        claims
        .join(
            policies,
            on="policy_no",
            how="left"
        )
    )


    # --------------------------------------------------------
    # CLAIMS + POLICIES + CUSTOMERS
    # --------------------------------------------------------

    customer_claim_policy_df = (
        claim_policy
        .join(
            customers,
            claim_policy["CUST_ID"] == customers["customer_id"],
            "left"
        )
        .drop(customers["customer_id"])
    )

    return customer_claim_policy_df


# ============================================================
# 3. CUSTOMER + CLAIM + POLICY + TELEMATICS
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.customer_claim_policy_telematics",
    comment="Customer, claims and policy data enriched with telematics",
    table_properties={
        "quality": "gold"
    }
)
def customer_claim_policy_telematics():

    # --------------------------------------------------------
    # READ PREVIOUS GOLD TABLE
    # --------------------------------------------------------

    customer_claim_policy = spark.read.table(
        f"{catalog}.{gold_schema}.customer_claim_policy"
    )


    # --------------------------------------------------------
    # READ AGGREGATED TELEMATICS
    # --------------------------------------------------------

    telematics = spark.read.table(
        f"{catalog}.{gold_schema}.aggregated_telematics"
    )


    # --------------------------------------------------------
    # OPTIONAL FILTER
    # Keep only claims having a borough
    # --------------------------------------------------------

    customer_claim_policy = (
        customer_claim_policy
        .filter(
            col("BOROUGH").isNotNull()
        )
    )


    # --------------------------------------------------------
    # JOIN USING VEHICLE CHASSIS
    # --------------------------------------------------------

    result = (
        customer_claim_policy
        .join(
            telematics,
            on="chassis_no",
            how="left"
        )
    )


    return result