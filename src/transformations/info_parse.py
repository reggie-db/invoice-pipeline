"""
Lakeflow stage that normalizes raw AI responses into typed struct columns.

This module serves as the final transformation in the invoice processing pipeline,
converting freeform JSON responses from the key information extraction (KIE)
endpoint into a schema aware struct suitable for analytics queries and joins.

Highlights:
    * Reads outputs from `info_extract` containing raw AI response payloads
    * Applies `infer_json_parse` to dynamically infer and coerce JSON schemas
    * Expands struct fields into top level columns for direct SQL access
    * Preserves `content_hash` and `path` for lineage tracking

Output Schema:
    The resulting table contains:
        - content_hash (string): SHA256 identifier linking back to source file
        - path (string): Original file path for provenance
        - [dynamic fields]: Extracted invoice fields (vendor, amount, date, etc.)
          determined by the KIE endpoint's response structure
"""

import dlt
from pyspark.sql import functions as F
from reggie_tools import funcs


@dlt.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def info_parse():
    """
    Parse and normalize key information extraction responses into typed columns.

    This function reads the streaming output from `info_extract`, which contains
    raw JSON responses from the configured AI endpoint. It applies schema inference
    to convert the freeform JSON into a strongly typed struct, then flattens the
    struct fields into individual columns.

    Processing Steps:
        1. Read streaming records from info_extract table
        2. Extract the result field from the AI response wrapper
        3. Apply infer_json_parse to derive struct schema from JSON content
        4. Select content_hash and path for lineage, plus all parsed fields

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame with columns:
            - content_hash: Unique file identifier for joins
            - path: Source file path for debugging and auditing
            - [parsed fields]: Dynamic columns based on KIE response structure

    Note:
        The actual columns produced depend on the KIE endpoint configuration.
        Common fields include vendor_name, invoice_number, invoice_date,
        line_items, and total_amount, but this varies by use case.
    """
    df = (
        spark.readStream.table("info_extract")
        # Convert the AI response JSON into a schema aware struct.
        # infer_json_parse examines the JSON content and builds a StructType
        # matching the nested structure, enabling downstream SQL access.
        .withColumn(
            "parse", funcs.infer_json_parse(F.expr("info.result").cast("string"))
        )
        .drop("info")
    )

    # Flatten parsed struct fields alongside provenance columns
    return df.select("content_hash", "path", "parse.*")
