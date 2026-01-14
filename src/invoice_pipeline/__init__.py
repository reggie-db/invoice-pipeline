"""
Invoice Pipeline: A Lakeflow streaming pipeline for invoice document processing.

This package contains the DLT table definitions that form the invoice processing
pipeline. Each module defines one or more streaming tables that transform invoice
files from raw binary uploads to structured, queryable data.

Modules:
    config: Configuration helper for accessing pipeline settings
    file_ingest: Entry point that streams files from Unity Catalog Volumes
    file_convert: Content normalization (e.g., SVG to PNG conversion)
    file_parse: Document parsing via ai_parse_document or pypdf fallback
    text_extract: Text flattening from parsed document structures
    info_extract: AI powered key information extraction
    info_parse: JSON response normalization into typed columns
"""
