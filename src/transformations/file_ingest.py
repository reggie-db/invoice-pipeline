import io
import logging
import mimetypes
from typing import Iterator

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from reggie_tools import configs

"""
Lakeflow pipeline module for ingesting files from a Unity Catalog Volume.

This module implements the entry point for the invoice processing pipeline,
streaming binary files from a configured Volume using Databricks Auto Loader.
It enriches each file record with computed metadata including content hashes
and MIME type detection via the Magika ML model.

Responsibilities:
    * Stream files from the target Volume using cloudFiles format
    * Compute SHA256 content hash for deduplication and lineage tracking
    * Detect MIME type and file extension using Magika's ML classifier
    * Deduplicate records based on content_hash to prevent reprocessing

Output Schema:
    - path (string): Full volume path to the source file
    - modificationTime (timestamp): File system modification timestamp
    - length (long): File size in bytes
    - content (binary): Raw file content
    - event_timestamp (timestamp): Processing timestamp
    - content_hash (string): SHA256 hash of content for deduplication
    - mime_type (string): Detected MIME type (e.g., application/pdf)
    - extension (string): Inferred file extension (e.g., pdf)
"""

print(f"log handlers: {logging.root.handlers}")

# ---------- UDFs ----------


@F.pandas_udf("mime_type string, extension string")
def file_info_udf(it: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Detect MIME type and best guess file extension from in memory binary content.

    This UDF processes streaming or batch data where files are stored as
    binary bytes in a content column (rather than on disk). It uses the
    Magika model to classify content and infer likely extensions.

    Args:
        it: Iterator of pandas DataFrames with columns:
            - path (string): File path or name for logging/context
            - content (binary): File content as bytes

    Yields:
        pandas DataFrames with columns:
            - mime_type (string): Detected MIME type
            - extension (string): Inferred file extension (may be None)

    Notes:
        Uses Magika's identify_stream() for efficient inference from bytes.
        Falls back to mimetypes.guess_type() when Magika fails.
        Logs warnings when Magika cannot identify a file.
    """
    from magika import Magika
    from reggie_core import logs

    log = logs.logger()
    m = Magika()

    def _mime_type_extension(
        path: str, content: bytes
    ) -> tuple[str | None, str | None]:
        """
        Determine MIME type and extension for a single file.

        Args:
            path: File path for fallback extension detection.
            content: Binary file content to analyze.

        Returns:
            Tuple of (mime_type, extension), either may be None.
        """
        if content:
            try:
                with io.BytesIO(content) as stream:
                    if stream is not None:
                        output = m.identify_stream(stream).output
                        # Magika returns extensions as list or single value
                        extension = (
                            output.extensions[0]
                            if (
                                isinstance(output.extensions, list)
                                and output.extensions
                            )
                            else output.extensions
                        )

                        return output.mime_type, extension

            except Exception as e:
                log.error(
                    f"magika failed to identify file - path:{path}",
                    e,
                )
        # Fallback: extract extension from filename and guess MIME type
        if filename := path.split("/")[-1] if path else None:
            extension = filename.split(".")[-1]
            mime_type, _ = mimetypes.guess_type(filename)
            return mime_type, extension
        return None, None

    for pdf in it:
        mime_types: list[str | None] = []
        extensions: list[str | None] = []

        for path, content in zip(pdf["path"], pdf["content"]):
            mime_type, extension = _mime_type_extension(path, content)
            mime_types.append(mime_type)
            extensions.append(extension)

        yield pd.DataFrame({"mime_type": mime_types, "extension": extensions})


# ---------- DLT Tables ----------


@dp.table(table_properties={})
def file_ingest():
    """
    Stream files from the Unity Catalog Volume and enrich with metadata.

    This function creates the primary ingestion streaming table for the pipeline.
    It uses Auto Loader (cloudFiles format) to incrementally process new files
    as they arrive in the configured Volume path.

    Configuration:
        Reads the following values from pipeline configuration:
        - catalog_name: Unity Catalog name
        - schema_name: Schema containing the volume
        - volume_name: Volume name to monitor
        - volume_path: Optional subfolder within the volume

    Returns:
        pyspark.sql.DataFrame: Streaming DataFrame with columns:
            - path: Source file path in the volume
            - modificationTime: File modification timestamp
            - length: File size in bytes
            - content: Raw binary content
            - event_timestamp: Processing timestamp (current time)
            - content_hash: SHA256 hash for deduplication
            - mime_type: Detected MIME type
            - extension: Inferred file extension

    Note:
        Records are deduplicated on content_hash to prevent reprocessing
        identical files that may be uploaded multiple times.
    """
    # Retrieve volume path configuration from pipeline settings
    catalog_name: str = configs.config_value("catalog_name")
    schema_name: str = configs.config_value("schema_name")
    volume_name: str = configs.config_value("volume_name")
    volume_path: str = configs.config_value("volume_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .load(
            f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
            + (f"/{volume_path}" if volume_path else "")
        )
        # Add processing timestamp for event time tracking
        .withColumn("event_timestamp", F.current_timestamp())
        # Compute content hash for deduplication and downstream joins
        .withColumn("content_hash", F.sha2(F.col("content"), 256))
        # Detect MIME type and extension using Magika ML model
        .withColumn(
            "file_info", file_info_udf(F.struct(F.col("path"), F.col("content")))
        )
        # Flatten file_info struct into top-level columns
        .select("*", "file_info.*")
        .drop("file_info")
        # Deduplicate on content hash to prevent reprocessing identical files
        .dropDuplicates(["content_hash"])
    )
