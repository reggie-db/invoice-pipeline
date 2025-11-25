from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""
Lakeflow stage that calls a knowledge extraction endpoint over normalized text.

Highlights:
* reads newline joined text from `text_extract`
* invokes `ai_query` to pull structured key information extraction (KIE) results
"""


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_extract():
    """
    Trigger AI powered key information extraction on normalized document text.

    Returns:
        Streaming DataFrame carrying original metadata and the `info` column.
    """

    read = spark.readStream.table("text_extract").withColumn(
        "info",
        F.expr("ai_query('kie-e031b1e0-endpoint',text,failOnError => false)"),
    )
    return read
