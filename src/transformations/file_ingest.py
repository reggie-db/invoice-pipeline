"""
Lakeflow pipeline module for ingesting files from a Unity Catalog Volume.

Responsibilities:
- Ensure the target Volume exists
- Stream files from the Volume using Auto Loader
- Compute a content hash
- Detect MIME type and extension with a vectorized Pandas UDF
"""

import json
from typing import Iterator
from typing import Optional, Any, Iterable
from pyspark import pipelines as dp
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F, types as T
import io
import logging
from reggie_tools import configs
from pathlib import Path
import mimetypes

print(f"log handlers: {logging.root.handlers}")

# ---------- UDFs ----------


@F.pandas_udf("mime_type string, extension string")
def file_info_udf(it: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Detect MIME type and best-guess file extension from in-memory binary content.

    Overview:
        This UDF processes streaming or batch data where files are stored as
        binary bytes in a 'content' column (rather than on disk). It uses the
        Magika model to classify content and infer likely extensions.

    Input:
        Iterator of pandas DataFrames with at least:
            - path (string): file path or name for logging/context
            - content (binary): file content as bytes

    Output:
        Yields pandas DataFrames with:
            - mime_type (string): detected MIME type
            - extension (string): inferred file extension (may be None)

    Notes:
        • Uses Magika’s `identify_bytes()` for efficient inference from bytes.
        • The optional `filename` hint improves detection for ambiguous data.
        • Logs warnings when Magika fails to identify a file.
    """
    import re
    from magika import Magika
    from reggie_core import logs

    log = logs.logger()
    m = Magika()

    def _mime_type_extension(
        path: str, content: bytes
    ) -> tuple[str | None, str | None]:
        if content:
            try:
                with io.BytesIO(content) as stream:
                    if stream is not None:
                        output = m.identify_stream(stream).output
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


# ---------- DLT tables ----------
@dp.table(table_properties={})
def file_ingest():
    """
    Stream files from the Volume using Auto Loader and enrich with metadata.

    Returns:
        A streaming DataFrame with:
            path: source file path
            modificationTime, length: file system metadata
            content_hash: SHA-256 hash of file content
            mime_type, extension: detected file metadata
    """
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
        .withColumn("event_timestamp", F.current_timestamp())
        .withColumn("content_hash", F.sha2(F.col("content"), 256))
        .withColumn(
            "file_info", file_info_udf(F.struct(F.col("path"), F.col("content")))
        )
        .select("*", "file_info.*")
        .drop("file_info")
        .dropDuplicates(["content_hash"])
    )
