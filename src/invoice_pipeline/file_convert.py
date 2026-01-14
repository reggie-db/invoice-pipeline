import logging
from functools import reduce
from typing import Callable, Dict, Optional

import pandas as pd
import resvg_py
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

"""
Lakeflow pipeline stage that normalizes binary file content for downstream parsing.

This module handles content transformation for file types that require conversion
before parsing. Currently supports SVG to PNG rasterization, with an extensible
registry pattern for adding new converters.

Highlights:
    * Streams records from the file_ingest table and filters by MIME prefix
    * Applies registered converters (currently SVG to PNG bytes) with error handling
    * Emits `content_hash`, `event_timestamp`, and normalized `content` columns
    * Uses pandas UDFs for distributed batch processing

Output Schema:
    - content_hash (string): SHA256 identifier linking to source file
    - event_timestamp (timestamp): Processing timestamp
    - content (binary): Converted content, or null if no conversion applied
"""

# ========= TYPES =========

# Type alias for converter functions: takes MIME type and content, returns converted bytes
ContentConverter = Callable[[str, bytes], Optional[bytes]]

# ========= CONFIGURATION =========

# Registry mapping MIME type prefixes to converter functions.
# Add new converters here to support additional file type invoice_pipeline.
CONTENT_CONVERTERS: Dict[str, ContentConverter] = {
    # SVG files are rasterized to PNG for consistent image processing
    "image/svg": lambda _, content: resvg_py.svg_to_bytes(
        svg_string=content.decode("utf-8", errors="replace")
    )
}


# ========= CONVERSION LOGIC =========


def convert_content(
        path: str, mime_type: Optional[str], content: Optional[bytes]
) -> Optional[bytes]:
    """
    Attempt to convert file content based on its MIME type.

    Iterates through registered converters and applies the first matching one.
    Logs diagnostic information for debugging conversion issues.

    Args:
        path: File path for diagnostic logging.
        mime_type: MIME string describing the file format (e.g., image/svg+xml).
        content: Raw binary data for the file.

    Returns:
        Converted binary data if a matching converter applies and succeeds.
        None if no converter matches, conversion fails, or content is empty.
    """

    log = logging.getLogger(__name__)
    log.info(f"Converting | path:{path}, mime_type:{mime_type}")

    if mime_type and content:
        for prefix, converter in CONTENT_CONVERTERS.items():
            # Skip converters that don't match the MIME prefix
            if not mime_type.startswith(prefix):
                continue
            try:
                converted = converter(mime_type, content)
                # Only return if conversion produced different content
                if converted is not None and converted != content:
                    log.info(
                        f"Conversion succeeded | path:{path}, mime_type:{mime_type}"
                    )
                    return converted
            except Exception as e:
                log.error(f"Conversion failed | path:{path}, mime_type:{mime_type}", e)
                continue

    log.warning(f"No applicable converter | path:{path}, mime_type:{mime_type}")
    return None


@F.pandas_udf(T.BinaryType())
def convert_content_udf(
        paths: pd.Series, mime_types: pd.Series, contents: pd.Series
) -> pd.Series:
    """
    Vectorized UDF wrapper for content conversion across distributed batches.

    Applies convert_content to each row in the batch, enabling parallel
    processing across Spark executors.

    Args:
        paths: Series of file paths for logging context.
        mime_types: Series of MIME type strings.
        contents: Series of binary content to convert.

    Returns:
        Pandas Series containing converted binary data or null values.
    """
    return pd.Series(
        [convert_content(p, m, c) for p, m, c in zip(paths, mime_types, contents)]
    )


# ========= PIPELINE DEFINITION =========


@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported",
    },
)
def file_convert():
    """
    Stream that normalizes and converts file content based on MIME type.

    Reads from the file_ingest table and applies content converters to files
    matching registered MIME prefixes. Non-matching files pass through with
    null content (they use their original content in downstream stages).

    Input:
        Reads from file_ingest table containing:
        - path, content, content_hash, mime_type, event_timestamp

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame with columns:
            - content_hash: Unique file identifier for joins
            - content: Converted binary data, or null if no conversion needed
            - event_timestamp: Processing timestamp

    Note:
        Files not matching any converter prefix will have null content.
        The file_parse stage uses the original content from file_ingest
        when the converted content is null.
    """

    # Build a combined filter condition matching any registered MIME prefix.
    # This determines which records are candidates for conversion.
    mime_type_col_filter = reduce(
        lambda a, b: a | b,
        (F.col("mime_type").like(f"{k}%") for k in CONTENT_CONVERTERS.keys()),
        F.lit(False),
    )
    read = (
        spark.readStream.table("file_ingest")
        .withColumn("event_timestamp", F.current_timestamp())
        .withColumn(
            "content",
            # Only attempt conversion for matching MIME types
            F.when(
                mime_type_col_filter,
                convert_content_udf(
                    F.col("path"),
                    F.col("mime_type"),
                    F.col("content"),
                ),
            ),
        )
    )

    return read.select("content_hash", "event_timestamp", "content")
