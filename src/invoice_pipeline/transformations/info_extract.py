from pyspark import pipelines as dp
from pyspark.sql import functions as F

from invoice_pipeline.transformations import config

"""
Lakeflow stage that invokes AI powered key information extraction (KIE).

This module serves as the bridge between document text extraction and structured
data output. It reads normalized text from the text_extract stage and calls a
configured AI endpoint to extract key fields such as vendor names, invoice
numbers, dates, line items, and totals.

Highlights:
    * Reads newline joined text from the `text_extract` streaming table
    * Invokes `ai_query` against a pre-deployed Agent Brick endpoint
    * Captures raw AI response in an `info` column for downstream parsing
    * Preserves all source columns (content_hash, path, text) for lineage

Configuration:
    Requires `information_extraction_endpoint` in pipeline configuration,
    which should be the identifier of a deployed KIE Agent Brick.

Output Schema:
    All columns from text_extract plus:
    - info (struct): Raw AI response containing extracted information
"""


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_extract():
    """
    Trigger AI powered key information extraction on normalized document text.

    This function reads streaming records from text_extract (containing flattened
    document text) and invokes the configured AI endpoint to extract structured
    invoice information. The AI response is captured raw for downstream parsing
    by the info_parse stage.

    Configuration:
        The `information_extraction_endpoint` utils value must point to a valid
        Agent Brick endpoint. Create this endpoint in your Databricks workspace
        before running the pipeline.

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame containing:
            - content_hash: Unique file identifier for joins
            - modificationTime: Source file timestamp
            - length: Source file size
            - path: Source file path
            - text: Extracted document text
            - info: AI response struct with extraction results

    Note:
        The ai_query function is called with failOnError=false to prevent
        individual extraction failures from blocking the entire stream.
        Failed extractions will have null info values.
    """
    # Retrieve the KIE endpoint identifier from pipeline configuration
    information_extraction_endpoint: str = config.get(
        "information_extraction_endpoint", dbutils=dbutils, spark=spark
    )

    read = spark.readStream.table("text_extract").withColumn(
        "info",
        # Call the AI endpoint; failOnError=false ensures stream continuity
        F.expr(
            f"ai_query('{information_extraction_endpoint}',text,failOnError => false)"
        ),
    )
    return read
