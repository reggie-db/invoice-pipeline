# Invoice Pipeline

This repository defines a Lakeflow style streaming pipeline that ingests invoice files, normalizes the binaries, parses their content, and extracts structured key information for downstream analytics.

## Core Flow
1. `file_ingest`: streams binary files from Unity Catalog Volumes via Auto Loader, adds `event_timestamp`, and dedupes with `content_hash`.
2. `file_convert`: filters supported MIME prefixes and applies converter functions (for example SVG to PNG bytes) before emitting normalized `content`.
3. `file_parse`: joins ingestion with converted payloads and runs `ai_parse_document` when supported, falling back to a PDF text routine otherwise.
4. `text_extract`: flattens parsed element arrays into a newline joined `text` column while keeping provenance fields such as `path` and `modificationTime`.
5. `info_extract`: calls the configured `ai_query` endpoint to run key information extraction on the normalized text.
6. `info_parse`: converts the raw AI response into a struct using `infer_json_parse`, which surfaces individual fields for querying.

## Repository Layout
* `src/transformations/`: Spark DLT tables used by the pipeline. Each module now includes documentation and targeted inline comments so you can understand the stage responsibilities quickly.
* `resources/pipeline.yml`: pipeline definition that wires the DLT tables together.
* `resources/uc.yml`: Unity Catalog configuration supporting the ingestion stage.

## Development Notes
* Configuration values such as catalog, schema, and volume live in `reggie_tools.configs` so update those settings before running the pipeline.
* The parsing stage automatically decides whether to use `ai_parse_document` based on the runtime version reported by `reggie_tools.runtimes`.
* All stages rely on streaming semantics, so validate the pipeline inside a Databricks or compatible Lakeflow workspace with the proper secrets already configured.
* Before deployment create an information extraction Agent Brick, then copy its endpoint identifier so you can pass it as the `information_extraction_endpoint` variable during execution.
* Supply a Unity Catalog name via the `catalog_name` variable when launching the bundle.

Example run command:

```
databricks bundle deploy --environment dev --profile FIELD-ENG-EAST --var "catalog_name=reggie_pierce" --var "information_extraction_endpoint=kie-e031b1e0-endpoint"
```

## Testing Checklist
* Run unit or integration tests (if available) after editing UDF logic.
* Verify that each streaming table starts successfully in your target workspace.
* Confirm the README instructions stay current whenever you add a new stage or configuration flag.

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
