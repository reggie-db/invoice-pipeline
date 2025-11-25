from pyspark.sql import functions as F
import dlt
from reggie_tools import funcs


@dlt.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_parse():
    df = (
        spark.readStream.table("info_extract")
        .withColumn(
            "parse", funcs.infer_json_parse(F.expr("info.result").cast("string"))
        )
        .drop("info")
    )


    return df.select("parse.*")
