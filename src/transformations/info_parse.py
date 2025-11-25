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

    # parse must be a struct and must contain value
    df = df.withColumn(
        "value",
        # F.when(
        #     (F.col("parse").isNotNull())
        #     & (F.col("parse").schema is not None)
        #     & (F.col("parse").schema.getField("value") is not None),
        #     F.col("parse")["value"],
        # ),
        F.struct(F.lit("example_0").alias("column_0"), F.lit("example_1").alias("column_1")),
    )

    return df.drop("parse").select("path", "value.*")
