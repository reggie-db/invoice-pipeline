# Invoice Pipeline

A Lakeflow (DLT) streaming pipeline that ingests invoice files from Unity Catalog Volumes, normalizes binary content, parses document structure, and extracts structured key information for downstream analytics.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Databricks Workspace | Must support Lakeflow Declarative Pipelines (serverless or classic) |
| Unity Catalog | Required for Volume based file ingestion and schema/table management |
| `reggie_tools` library | Installed automatically via bundle; provides `configs`, `runtimes`, and `funcs` helpers |
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
| `src/transformations/` | Spark DLT table definitions. Each module contains docstrings and inline comments explaining stage responsibilities. |
| `resources/pipeline.yml` | Lakeflow pipeline definition wiring DLT tables together with dependencies and configuration. |
| `resources/uc.yml` | Unity Catalog resource definitions (schema and volume) for the ingestion stage. |
| `databricks.yml` | Databricks Asset Bundle root configuration defining variables, targets, and includes. |

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

* Configuration values such as catalog, schema, and volume are read at runtime via `reggie_tools.configs`. Update pipeline configuration or bundle variables before running.
* The parsing stage (`file_parse`) automatically selects `ai_parse_document` vs. PDF fallback based on the runtime version reported by `reggie_tools.runtimes`.
* All stages rely on streaming semantics. Validate the pipeline inside a Databricks workspace with proper secrets and Unity Catalog permissions.
* The pipeline runs in serverless mode with Photon enabled by default. See `resources/pipeline.yml` to adjust compute settings.

## Testing Checklist

- [ ] Run unit or integration tests (if available) after editing UDF logic.
- [ ] Verify each streaming table starts successfully in your target workspace.
- [ ] Confirm MIME detection works for your invoice file types.
- [ ] Validate AI endpoint connectivity before running `info_extract`.
- [ ] Update this README whenever you add a new stage or configuration flag.

## License

See [LICENSE](LICENSE) for details.
