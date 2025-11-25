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

## Example Json

```json
{
  "invoice": {
    "invoiceNumber": "MB66680464",
    "invoiceDate": "2025-04-11",
    "purchaseOrderNumber": "PO-1000793",
    "salesOrderNumber": "AAA3718812",
    "amountDue": {
      "currency": "USD",
      "value": 27207.30
    },
    "terms": "Net 30 Days",
    "dueDate": "2025-05-11"
  },

  "seller": {
    "name": "Apple Inc.",
    "address": [
      "P.O. Box 846095",
      "Dallas TX 75284-6095",
      "USA"
    ]
  },

  "buyer": {
    "name": "Inspire Brands Inc",
    "address": [
      "3 Glenlake Pkwy FL 100",
      "Atlanta GA 30328-3584",
      "USA"
    ]
  },

  "shipTo": {
    "name": "Inspire Brands Inc",
    "attention": "Terence Williams",
    "address": [
      "3 Glenlake Pkwy FL 100",
      "Atlanta GA 30328-3584",
      "USA"
    ]
  },

  "lineItems": [
    {
      "lineNumber": "000010",
      "manufacturerPartNumber": "FRX63LL/A",
      "description": "RFB MBP 14 SL 11C 14C GPU 18G 512G USA",
      "quantityOrdered": 10,
      "quantityShipped": 10,
      "unitPrice": 1599.00,
      "extendedPrice": 15990.00,
      "serialNumbers": [
        "C5JC372NQ6",
        "CNW03FHVCM",
        "GVHFMWR72D",
        "H3JMWLDH6Q",
        "H4PYJ5FVJY",
        "H9HP4FLXYN",
        "JJKXRVVWPL",
        "K7WGH0CMY0",
        "KQQMGHQJ2Y",
        "M46DTW0FFY"
      ]
    },
    {
      "lineNumber": "000020",
      "manufacturerPartNumber": "SL9E2LL/A",
      "description": "AC+ 14 IN MB PRO M3 PRO M3 MAX PHX",
      "quantityOrdered": 10,
      "quantityShipped": 10,
      "unitPrice": 237.00,
      "extendedPrice": 2370.00,
      "serialNumbers": []
    },
    {
      "lineNumber": "000030",
      "manufacturerPartNumber": "FRW13LL/A",
      "description": "RFB MBP 16 SB 12C 18C GPU 18G 512G USA",
      "quantityOrdered": 3,
      "quantityShipped": 3,
      "unitPrice": 2039.00,
      "extendedPrice": 6117.00,
      "serialNumbers": [
        "G3597JM7LH",
        "GCXN3JD2K5",
        "L3XDQ329Y6"
      ]
    }
  ],

  "totals": {
    "subtotal": 25494.00,
    "tax": 1713.30,
    "shipping": 0.00,
    "total": 27207.30,
    "currency": "USD"
  }
}
```
