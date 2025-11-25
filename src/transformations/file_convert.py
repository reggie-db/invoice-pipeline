"""
Lakeflow pipeline module for converting file content to a normalized binary format.

Overview
---------
This module reads files and applies type-specific content conversions when applicable.
Each row in the output includes a conversion timestamp (`event_time`) that can be
used by downstream pipelines for event-time processing and joins.

Responsibilities
----------------
• Stream input files from an upstream source table
• Identify supported MIME types based on registered converters
• Apply the corresponding converter function to binary content
• Log all conversion attempts and outcomes for observability
• Produce a structured output table containing:
    - `content_hash`: string — unique file identifier
    - `converted_content`: binary — normalized or transformed file bytes
    - `event_time`: timestamp — derived from file metadata or processing time
"""

from typing import Dict, Callable, Optional

from functools import reduce
import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
import resvg_py

# ---------- TYPES ----------

ContentConverter = Callable[[str, bytes], Optional[bytes]]

# ---------- CONFIGURATION ----------

# Registry of MIME-type prefixes mapped to converter functions
CONTENT_CONVERTERS: Dict[str, ContentConverter] = {
    "image/svg": lambda _, content: resvg_py.svg_to_bytes(
        svg_string=content.decode("utf-8", errors="replace")
    )
}

# Build OR condition for all MIME prefixes
MIME_TYPE_COL_FILTER = reduce(
    lambda a, b: a | b,
    (F.col("mime_type").like(f"{k}%") for k in CONTENT_CONVERTERS.keys()),
    F.lit(False),
)

# ---------- CONVERSION LOGIC ----------


def convert_content(
    path: str, mime_type: Optional[str], content: Optional[bytes]
) -> Optional[bytes]:
    """
    Attempt to convert the provided file content based on MIME type.

    Args:
        path: File path for diagnostic logging.
        mime_type: MIME string describing the file format.
        content: Raw binary data for the file.

    Returns:
        Converted binary data if a matching converter applies and succeeds.
        None if no converter matches or conversion fails.
    """
    from reggie_core import logs

    log = logs.logger()
    log.info(f"Converting — path:{path}, mime_type:{mime_type}")
    if mime_type and content:

        for prefix, converter in CONTENT_CONVERTERS.items():
            if not mime_type.startswith(prefix):
                continue
            try:
                converted = converter(mime_type, content)
                if converted is not None and converted != content:
                    log.info(
                        f"Conversion succeeded — path:{path}, mime_type:{mime_type}"
                    )
                    return converted
            except Exception as e:
                log.error(f"Conversion failed — path:{path}, mime_type:{mime_type}", e)
                continue
    log.warning(f"No applicable converter — path:{path}, mime_type:{mime_type}")
    return None


@F.pandas_udf(T.BinaryType())
def convert_content_udf(
    paths: pd.Series, mime_types: pd.Series, contents: pd.Series
) -> pd.Series:
    """
    Applies all registered converters in parallel across distributed batches.

    Returns:
        Pandas Series containing converted binary data or null values.
    """
    return pd.Series(
        [convert_content(p, m, c) for p, m, c in zip(paths, mime_types, contents)]
    )


# ---------- PIPELINE DEFINITION ----------


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def file_convert():
    """
    Stream that normalizes and converts file content based on MIME type.

    Input:
        Reads from a table containing file metadata, MIME type, and binary content.

    Output:
        Emits a structured dataset containing:
            • `content_hash` — unique file identifier
            • `converted_content` — normalized binary data or null if unchanged
            • `event_time` — timestamp derived from processing time
    """
    read = (
        spark.readStream.table("file_ingest")
        .withColumn("event_timestamp", F.current_timestamp())
        .withColumn(
            "content",
            F.when(
                MIME_TYPE_COL_FILTER,
                convert_content_udf(
                    F.col("path"),
                    F.col("mime_type"),
                    F.col("content"),
                ),
            ),
        )
    )

    return read.select("content_hash", "event_timestamp", "content")
