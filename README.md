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


