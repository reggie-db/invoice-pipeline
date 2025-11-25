import io
import json
import re

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from reggie_tools import runtimes

"""
Lakeflow stage that parses binary documents into structured text elements.

Highlights:
* joins ingestion and conversion outputs to align on identical content_hash values
* prefers ai_parse_document on supported runtimes and falls back to PDF parsing
* emits the parsed payload as a variant column for downstream enrichment
"""

# Determine at import time whether ai_parse_document is available.
_supports_ai_parse = runtimes.version().startswith("17")


def extract_text_from_pdf(content) -> str:
    """
    Parse PDF bytes into a compact JSON payload when AI parsing is unavailable.

    The routine strips blank lines to reduce payload size while keeping ordering.
    """
    from pypdf import PdfReader

    text_parts = []
    with io.BytesIO(content) as stream:
        reader = PdfReader(stream)

        for page in reader.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)

    text = "\n".join(text_parts)
    text = re.sub(r"\n+", "\n", text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    parsed = {
        "document": {"elements": [{"content": ("\n".join(lines)), "type": "text"}]}
    }
    return json.dumps(parsed)


@F.pandas_udf(T.StringType())
def extract_text_from_pdf_udf(contents: pd.Series) -> pd.Series:
    """Vectorized wrapper around extract_text_from_pdf for streaming workloads."""
    return pd.Series([extract_text_from_pdf(c) for c in contents])


@dp.table(
    table_properties={
        "quality": "silver",
        "delta.feature.variantType-preview": "supported",
    },
)
def file_parse():
    """
    Join ingestion and conversion streams, then parse the binary payloads.

    Returns:
        DataFrame containing:
            * content_hash: unique identifier carried from ingestion
            * parsed: variant document tree from ai_parse_document or PDF fallback
    """
    ingest = (
        spark.readStream.table("file_ingest")
        .select(
            "content_hash",
            "content",
        )
        .alias("ingest")
    )

    conv = (
        spark.readStream.table("file_convert")
        .select(
            "content_hash",
            "content",
        )
        .alias("conv")
    )

    cond = F.expr("ingest.content_hash = conv.content_hash")

    # Align records only when both raw and converted payloads exist.
    joined = ingest.join(conv, on=cond, how="inner")

    if _supports_ai_parse:
        # Runtime seventeen introduces ai_parse_document with version flag.
        parsed_expr = F.expr(
            """
            ai_parse_document(
              ingest.content,
              map('version', '2.0')
            )
            """
        )
    else:
        # Fallback to PDF text extraction when AI parsing is unavailable.
        parsed_expr = F.try_parse_json(
            extract_text_from_pdf_udf(F.col("ingest.content"))
        )

    parsed = joined.withColumn("parsed", parsed_expr)

    return parsed.select(
        F.col("ingest.content_hash").alias("content_hash"),
        F.col("parsed"),
    )
