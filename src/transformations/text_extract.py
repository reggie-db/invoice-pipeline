from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def text_extract():
    if False:
        return spark.readStream.table("file_ingest")

    ingest = spark.readStream.table("file_ingest").alias("ingest")

    parse = (
        spark.readStream.table("file_parse")
        .select(
            "content_hash",
            "parsed",
        )
        .alias("parse")
    )

    cond = F.expr("ingest.content_hash = parse.content_hash")

    joined = (
        ingest.join(parse, on=cond, how="inner")
        .withColumn(
            "elements",
            F.expr(
                "cast(variant_get(parse.parsed, '$.document.elements') as array<variant>)"
            ),
        )
        .withColumn(
            "text",
            F.expr(
                """
                    array_join(transform(elements, x -> cast(variant_get(x, '$.content') as string)), '\n')
                    """
            ),
        )
    )

    return joined.select(
        "ingest.content_hash",
        "ingest.modificationTime",
        "ingest.length",
        "ingest.path",
        "text",
    )
