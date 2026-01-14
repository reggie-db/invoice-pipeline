# Invoice Pipeline

A Lakeflow (DLT) streaming pipeline that ingests invoice files from Unity Catalog Volumes, normalizes binary content, parses document structure, and extracts structured key information for downstream analytics.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Databricks Workspace | Must support Lakeflow Declarative Pipelines (serverless or classic) |
| Unity Catalog | Required for Volume based file ingestion and schema/table management |
| AI Functions | `ai_parse_document` available on runtime 17+; `ai_query` requires a deployed Agent endpoint |

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Unity Catalog Volume                                │
│                    /Volumes/{catalog}/{schema}/{volume}                     │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ Auto Loader (cloudFiles)
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  file_ingest                                                                 │
│  Streams binary files, computes content_hash, detects MIME type/extension   │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
              ┌────────────────────┴────────────────────┐
              ▼                                         ▼
┌─────────────────────────────┐           ┌─────────────────────────────┐
│  file_convert               │           │  (passthrough for parse)    │
│  Normalizes SVG → PNG bytes │           │                             │
└─────────────────────────────┘           └─────────────────────────────┘
              │                                         │
              └────────────────────┬────────────────────┘
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  file_parse                                                                  │
│  Joins ingest + convert; calls ai_parse_document (or PDF fallback)          │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  text_extract                                                                │
│  Flattens parsed elements into newline joined text column                   │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  info_extract                                                                │
│  Invokes ai_query against configured KIE endpoint                           │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  info_parse                                                                  │
│  Parses JSON response into typed struct columns                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Core Flow

1. **file_ingest**: Streams binary files from Unity Catalog Volumes via Auto Loader, adds `event_timestamp`, computes `content_hash` (SHA256), and deduplicates on hash. Detects MIME type and extension using the Magika model.
2. **file_convert**: Filters records matching registered MIME prefixes (currently `image/svg`) and applies converter functions (SVG to PNG bytes) before emitting normalized `content`.
3. **file_parse**: Joins ingestion with converted payloads and runs `ai_parse_document` when supported (runtime 17+), falling back to a pypdf text routine otherwise.
4. **text_extract**: Flattens parsed element arrays into a newline joined `text` column while preserving provenance fields such as `path` and `modificationTime`.
5. **info_extract**: Calls the configured `ai_query` endpoint to run key information extraction on the normalized text.
6. **info_parse**: Converts the raw AI response into a struct using `infer_json_parse`, surfacing individual fields for querying.

## Repository Layout

| Path | Description |
|------|-------------|
| `src/invoice_pipeline/` | Spark DLT table definitions. Each module contains docstrings and inline comments explaining stage responsibilities. |
| `src/invoice_pipeline/config.py` | Configuration helper that reads values from widgets, Spark conf, or environment variables. |
| `resources/pipeline.yml` | Lakeflow pipeline definition wiring DLT tables together with dependencies and configuration. |
| `resources/uc.yml` | Unity Catalog resource definitions (schema and volume) for the ingestion stage. |
| `databricks.yml` | Databricks Asset Bundle root configuration defining variables, targets, and includes. |
| `requirements.txt` | Python dependencies (magika, pypdf) installed in the pipeline cluster environment. |

## Configuration Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `catalog_name` | Yes | None | Unity Catalog name where tables and volumes reside |
| `schema_name` | No | `${bundle.name}_${bundle.target}` | Schema name; production omits target suffix |
| `volume_name` | No | `files` | Volume containing source invoice files |
| `information_extraction_endpoint` | Yes | None | Agent Brick endpoint identifier for KIE |

## Deployment

### 1. Create the KIE Agent Brick

Before deploying the pipeline, create an Agent Brick configured for key information extraction. Copy the resulting endpoint identifier (e.g., `kie-e031b1e0-endpoint`).

### 2. Deploy the Bundle

```bash
databricks bundle deploy \
  --target dev \
  --profile YOUR_PROFILE \
  --var "catalog_name=your_catalog" \
  --var "information_extraction_endpoint=your-kie-endpoint"
```

For production:

```bash
databricks bundle deploy \
  --target prod \
  --profile YOUR_PROFILE \
  --var "catalog_name=your_catalog" \
  --var "information_extraction_endpoint=your-kie-endpoint"
```

### 3. Upload Test Files

Drop invoice files (PDF, SVG, images) into the configured Volume path:

```
/Volumes/{catalog_name}/{schema_name}/{volume_name}/
```

The pipeline will automatically ingest new files via Auto Loader.

## Development Notes

* Configuration values such as catalog, schema, and volume are read at runtime via `invoice_pipeline.config.get()`. This helper checks widgets, Spark conf, and environment variables in order.
* The parsing stage (`file_parse`) automatically selects `ai_parse_document` vs. PDF fallback based on the `DATABRICKS_RUNTIME_VERSION` environment variable (runtime 17+ required for AI parsing).
* All stages rely on streaming semantics. Validate the pipeline inside a Databricks workspace with proper secrets and Unity Catalog permissions.
* The pipeline runs in serverless mode with Photon enabled by default. See `resources/pipeline.yml` to adjust compute settings.

## Testing Checklist

- [ ] Run unit or integration tests (if available) after editing UDF logic.
- [ ] Verify each streaming table starts successfully in your target workspace.
- [ ] Confirm MIME detection works for your invoice file types.
- [ ] Validate AI endpoint connectivity before running `info_extract`.
- [ ] Update this README whenever you add a new stage or configuration flag.

## Example JSON

```json
{
    "invoice": {
        "invoiceNumber": "ZX90231455",
        "invoiceDate": "2024-11-19",
        "purchaseOrderNumber": "PO-4482917",
        "salesOrderNumber": "SO-99218344",
        "amountDue": {"currency": "USD", "value": 18492.75},
        "terms": "Net 30 Days",
        "dueDate": "2024-12-19"
    },

    "seller": {
        "name": "Pinecrest Systems",
        "address": ["912 Maple Terrace", "Columbus OH 43004", "USA"]
    },

    "buyer": {
        "name": "Northwind Holdings",
        "address": ["87 Riverbend Plaza Suite 500", "Charlotte NC 28202", "USA"]
    },

    "shipTo": {
        "name": "Northwind Holdings",
        "attention": "Jordan Parker",
        "address": ["87 Riverbend Plaza Suite 500", "Charlotte NC 28202", "USA"]
    },

    "lineItems": [
        {
            "lineNumber": "000010",
            "manufacturerPartNumber": "PXE21KJ",
            "description": "ULTRA LAPTOP 14 GEN5 16G 512G GRAPHITE",
            "quantityOrdered": 10,
            "quantityShipped": 10,
            "unitPrice": 1299.00,
            "extendedPrice": 12990.00,
            "serialNumbers": [
                "S9T4M3K2QW", "H2LM7C8RZS", "QR5T9W2PJL", "B1KXM8Q2FJ", "Z4N7C2LMKD", "KQ2XP9S4TM",
                "T7W9D3QFVB", "F5H9KL2XCM", "P2Z8Q7H3LW", "M9T2K4QXFS"
            ]
        },
        {
            "lineNumber": "000020",
            "manufacturerPartNumber": "LK92MZQ",
            "description": "CARE PACKAGE PREMIUM COVERAGE GEN5",
            "quantityOrdered": 10,
            "quantityShipped": 10,
            "unitPrice": 199.00,
            "extendedPrice": 1990.00,
            "serialNumbers": []
        },
        {
            "lineNumber": "000030",
            "manufacturerPartNumber": "XT41QPL",
            "description": "ULTRA LAPTOP 16 GEN5 32G 1TB GRAPHITE",
            "quantityOrdered": 3,
            "quantityShipped": 3,
            "unitPrice": 1799.00,
            "extendedPrice": 5397.00,
            "serialNumbers": ["D8K4X1QFMA", "Z1Q7F5W9RT", "M6T2HP4QGS"]
        }
    ],

    "totals": {
        "subtotal": 20377.00,
        "tax": 1115.75,
        "shipping": 0.00,
        "total": 18492.75,
        "currency": "USD"
    }
}
```

## License

See [LICENSE](LICENSE) for details.
