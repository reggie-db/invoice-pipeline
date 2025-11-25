import io
import json
import re

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from reggie_tools import runtimes

_supports_ai_parse = runtimes.version().startswith("17")


def extract_text_from_pdf(content) -> str:
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
    return pd.Series([extract_text_from_pdf(c) for c in contents])


@dp.table(
    table_properties={
        "quality": "silver",
        "delta.feature.variantType-preview": "supported",
    },
)
def file_parse():
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
