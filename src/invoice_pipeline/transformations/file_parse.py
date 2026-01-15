import io
import json
import os
import re

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from packaging.version import Version

"""
Lakeflow stage that parses binary documents into structured text elements.

This module joins the ingestion and conversion streams, then applies document
parsing to extract text and structural information. It automatically selects
between Databricks AI Functions (ai_parse_document) on supported runtimes and
a pypdf based fallback for older environments.

Highlights:
    * Joins file_ingest and file_convert outputs on content_hash
    * Prefers ai_parse_document on runtime 17+ for superior document understanding
    * Falls back to pypdf text extraction for PDF files on older runtimes
    * Emits parsed payload as a variant column for flexible downstream processing

Output Schema:
    - content_hash (string): SHA256 identifier linking to source file
    - parsed (variant): Structured document tree with elements array
"""


def extract_text_from_pdf(content) -> str:
    """
    Parse PDF bytes into a compact JSON payload when AI parsing is unavailable.

    Uses pypdf to extract text from each page, then formats it into the same
    JSON structure that ai_parse_document produces for consistency.

    Args:
        content: Binary PDF content to parse.

    Returns:
        JSON string with structure: {"document": {"elements": [{"content": "...", "type": "text"}]}}
        This matches the ai_parse_document output schema for downstream compatibility.
    """
    from pypdf import PdfReader

    text_parts = []
    with io.BytesIO(content) as stream:
        reader = PdfReader(stream)

        for page in reader.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)

    # Join pages and normalize whitespace
    text = "\n".join(text_parts)
    text = re.sub(r"\n+", "\n", text)  # Collapse multiple newlines
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    # Wrap in document structure matching ai_parse_document output
    parsed = {
        "document": {"elements": [{"content": ("\n".join(lines)), "type": "text"}]}
    }
    return json.dumps(parsed)


@F.pandas_udf(T.StringType())
def extract_text_from_pdf_udf(contents: pd.Series) -> pd.Series:
    """
    Vectorized wrapper around extract_text_from_pdf for streaming workloads.

    Args:
        contents: Series of binary PDF content.

    Returns:
        Series of JSON strings containing parsed document structure.
    """
    return pd.Series([extract_text_from_pdf(c) for c in contents])


@dp.table(
    table_properties={
        "quality": "silver",
        "delta.feature.variantType-preview": "supported",
    },
)
def file_parse():
    """
    Join ingestion and conversion streams, then parse binary payloads into structured text.

    This function performs a streaming inner join between file_ingest (raw files)
    and file_convert (normalized content) on content_hash. It then applies document
    parsing to extract text and structural elements.

    The parsing method is selected automatically:
        - Runtime 17+: Uses ai_parse_document with version 2.0 for best results
        - Older runtimes: Falls back to pypdf text extraction

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame with columns:
            - content_hash: Unique file identifier for downstream joins
            - parsed: Variant column containing document structure

    Note:
        The join requires both raw and converted records to exist, ensuring
        that files have passed through the conversion stage even if no
        actual conversion was applied.
    """
    # Read raw content from ingestion stage
    ingest = (
        spark.readStream.table("file_ingest")
        .select(
            "content_hash",
            "content",
        )
        .alias("ingest")
    )

    # Read converted content (may be null if no conversion applied)
    conv = (
        spark.readStream.table("file_convert")
        .select(
            "content_hash",
            "content",
        )
        .alias("conv")
    )

    cond = F.expr("ingest.content_hash = conv.content_hash")

    # Inner join ensures both streams have processed the file
    joined = ingest.join(conv, on=cond, how="inner")

    if _supports_ai_parse():
        # Runtime 17+ supports ai_parse_document with enhanced document understanding
        parsed_expr = F.expr(
            """
            ai_parse_document(
              ingest.content,
              map('version', '2.0')
            )
            """
        )
    else:
        # Fallback to pypdf text extraction on older runtimes
        parsed_expr = F.try_parse_json(
            extract_text_from_pdf_udf(F.col("ingest.content"))
        )

    parsed = joined.withColumn("parsed", parsed_expr)

    return parsed.select(
        F.col("ingest.content_hash").alias("content_hash"),
        F.col("parsed"),
    )


# Determine at import time whether ai_parse_document is available.
# Runtime 17+ includes the AI Functions preview feature.
def _supports_ai_parse():
    databricks_runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None)
    if databricks_runtime_version:
        runtime_version = Version(databricks_runtime_version)
        if runtime_version >= Version("17"):
            return True
    return False
