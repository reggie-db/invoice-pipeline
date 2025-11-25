from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_extract():

    read = spark.readStream.table("text_extract").withColumn(
        "info",
        F.expr("ai_query('kie-e031b1e0-endpoint',text,failOnError => false)"),
    )
    return read
