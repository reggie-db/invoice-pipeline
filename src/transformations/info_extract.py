from pyspark.sql import functions as F
import dlt


@dlt.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_extract():

    read = dlt.read_stream("text_extract").withColumn(
        "info",
        F.expr("ai_query('kie-e031b1e0-endpoint',text,failOnError => false)"),
    )
    return read
