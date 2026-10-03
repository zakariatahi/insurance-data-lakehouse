from pyspark import pipelines as dp
@dp.table(
    name="smart_claims_catalog.01_bronze.training_images",
    comment="Raw accident training image ingested from ADLS GEN2", 
    table_properties={"quality": "bronze"}
)
def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")
        .load(f"/Volumes/smart_claims_catalog/00_landing/training-imgs"))