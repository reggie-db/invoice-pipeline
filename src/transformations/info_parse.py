import dlt
from pyspark.sql import functions as F
from reggie_tools import funcs

"""
Lakeflow stage that turns raw KIE responses into normalized columns.

Highlights:
* reads outputs from `info_extract`
* relies on `infer_json_parse` to coerce freeform JSON into a struct schema
"""


@dlt.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_parse():
    """
    Normalize `info_extract` responses into typed columns for analytics use.
    """
    df = (
        spark.readStream.table("info_extract")
        # Convert the AI response into a schema-aware struct for downstream joins.
        .withColumn(
            "parse", funcs.infer_json_parse(F.expr("info.result").cast("string"))
        )
        .drop("info")
    )

    return df.select("content_hash", "path", "parse.*")
