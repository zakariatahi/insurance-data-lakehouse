from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# ---------------------------------------------------------
# Event Hubs configuration
# ---------------------------------------------------------

EVENTHUB_NAMESPACE = "smart-claims-streaming"
EVENTHUB_NAME = "telematics"

EVENTHUB_CONNECTION_STRING = spark.conf.get("eventhub.connectionString")


bootstrap_servers = (
    f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
)

sasl_jaas_config = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f'password="{EVENTHUB_CONNECTION_STRING}";'
)


# ---------------------------------------------------------
# JSON payload schema
# ---------------------------------------------------------

payload_schema = StructType([
    StructField("chassis_no", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True)
])


# ---------------------------------------------------------
# Bronze streaming table
# ---------------------------------------------------------

@dp.table(
    name="smart_claims_catalog.01_bronze.telematics"
)
def bronze_telematics():

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers
        )
        .option(
            "subscribe",
            EVENTHUB_NAME
        )
        .option(
            "kafka.security.protocol",
            "SASL_SSL"
        )
        .option(
            "kafka.sasl.mechanism",
            "PLAIN"
        )
        .option(
            "kafka.sasl.jaas.config",
            sasl_jaas_config
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
    )

    parsed_stream = (
        raw_stream

        # Convert Kafka binary payload to string
        .selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )

        # Parse JSON payload
        .withColumn(
            "decoded_data",
            from_json(
                col("raw_json"),
                payload_schema
            )
        )
        .withColumn(
            "stream_metadata",
            struct(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp")
            )
        )
    )


    

    return parsed_stream.select(
        col("decoded_data.chassis_no").alias("chassis_no"),
        col("decoded_data.latitude").alias("latitude"),
        col("decoded_data.longitude").alias("longitude"),
        col("decoded_data.event_timestamp").alias("event_timestamp"),
        col("decoded_data.speed").alias("speed"),
        col("stream_metadata")
    )
