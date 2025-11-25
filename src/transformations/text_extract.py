from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""
Lakeflow stage that extracts plain text blobs from parsed document variants.

Highlights:
* joins ingestion metadata with parsed payloads to preserve file lineage
* flattens element arrays into newline delimited text for AI downstream tasks
"""


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def text_extract():
    """
    Join parsed document variants with ingestion metadata and emit unified text.

    Returns:
        DataFrame containing metadata columns plus reconstructed text content.
    """
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
        # Extract the JSON array that stores ordered document elements.
        .withColumn(
            "elements",
            F.expr(
                "cast(variant_get(parse.parsed, '$.document.elements') as array<variant>)"
            ),
        )
        # Collapse element content into a newline separated string for readability.
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
