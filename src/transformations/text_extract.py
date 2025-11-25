from pyspark.sql import functions as F
import dlt


@dlt.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def text_extract():

    if False:
        return dlt.read_stream("file_ingest")

    ingest = dlt.read_stream("file_ingest").alias("ingest")

    parse = (
        dlt.read_stream("file_parse")
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
