from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""
Lakeflow stage that extracts plain text from parsed document variants.

This module joins ingestion metadata with parsed document payloads and flattens
the structured element arrays into a single text column. The resulting text
is suitable for downstream AI processing such as key information extraction.

Highlights:
    * Joins file_ingest (metadata) with file_parse (parsed content) on content_hash
    * Extracts the elements array from the parsed variant structure
    * Flattens element content into newline delimited text for AI consumption
    * Preserves file provenance columns for lineage tracking

Output Schema:
    - content_hash (string): SHA256 identifier for joins and deduplication
    - modificationTime (timestamp): Source file modification time
    - length (long): Source file size in bytes
    - path (string): Source file path for auditing
    - text (string): Extracted document text, newline separated
"""


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def text_extract():
    """
    Join parsed document variants with ingestion metadata and emit unified text.

    Performs a streaming inner join between file_ingest (containing file metadata)
    and file_parse (containing parsed document structure). Extracts the content
    from each document element and joins them into a single text column.

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame with columns:
            - content_hash: Unique file identifier
            - modificationTime: File modification timestamp from volume
            - length: File size in bytes
            - path: Full volume path to source file
            - text: Extracted text content, elements joined by newlines

    Note:
        The text column concatenates all element content fields from the
        parsed document structure, separated by newlines. This provides
        a flat text representation suitable for LLM based extraction.
    """
    # Read file metadata from ingestion stage
    ingest = spark.readStream.table("file_ingest").alias("ingest")

    # Read parsed document structure
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
        # Extract elements array from the parsed variant JSON structure
        .withColumn(
            "elements",
            F.expr(
                "cast(variant_get(parse.parsed, '$.document.elements') as array<variant>)"
            ),
        )
        # Transform elements array: extract content field from each, join with newlines
        .withColumn(
            "text",
            F.expr(
                """
                    array_join(transform(elements, x -> cast(variant_get(x, '$.content') as string)), '\n')
                    """
            ),
        )
    )

    # Select provenance columns plus extracted text
    return joined.select(
        "ingest.content_hash",
        "ingest.modificationTime",
        "ingest.length",
        "ingest.path",
        "text",
    )
