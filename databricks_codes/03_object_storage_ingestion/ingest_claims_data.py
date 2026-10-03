from pyspark import pipelines as dp
@dp.table(
    name="claim_images_meta",
    comment="Raw accident claim images metadata ingested from ADLS gen2", 
    table_properties={"quality": "bronze"}
)
def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaevolutionMode", "rescue")
        .load(f"/Volumes/smart_claims_catalog/00_landing/claims/metadata"))