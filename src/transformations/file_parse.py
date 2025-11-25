from pyspark.sql import functions as F, types as T

import dlt
import pandas as pd
import ipython_genutils
import io
import re
import json


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


@dlt.table(
    table_properties={
        "quality": "silver",
        "delta.feature.variantType-preview": "supported",
    },
)
def file_parse():

    ingest = (
        dlt.read_stream("file_ingest")
        .select(
            "content_hash",
            "content",
        )
        .alias("ingest")
    )

    conv = (
        dlt.read_stream("file_convert")
        .select(
            "content_hash",
            "content",
        )
        .alias("conv")
    )

    cond = F.expr("ingest.content_hash = conv.content_hash")

    joined = ingest.join(conv, on=cond, how="inner")

    parsed = joined.withColumn(
        "parsed",
        # F.when(
        #     F.expr("conv.content is not null"),
        #     F.expr("ai_parse_document(coalesce(conv.content))"),
        # ).otherwise(
        #     F.try_parse_json(extract_text_from_pdf_udf(F.expr("ingest.content")))
        # ),
        F.try_parse_json(extract_text_from_pdf_udf(F.expr("ingest.content")))
    )

    return parsed.select(
        F.col("ingest.content_hash").alias("content_hash"),
        F.col("parsed"),
    )
