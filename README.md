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

## Example Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Generated Schema",
  "type": "object",
  "properties": {
    "invoice": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "invoiceNumber": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "A unique alphanumeric identifier assigned by the seller to distinguish this invoice from others. Format follows pattern of letters followed by numbers (e.g., MB66680464)."
            },
            "invoiceDate": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The date when the invoice was issued, formatted as YYYY-MM-DD."
            },
            "purchaseOrderNumber": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The buyer's purchase order number that this invoice references, formatted with prefix 'PO-' followed by numeric digits."
            },
            "salesOrderNumber": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The seller's internal sales order number that corresponds to this invoice, consisting of alphanumeric characters."
            },
            "amountDue": {
              "anyOf": [
                {
                  "type": "object",
                  "properties": {
                    "currency": {
                      "anyOf": [
                        {
                          "type": "string"
                        },
                        {
                          "type": "null"
                        }
                      ],
                      "description": "The three-letter ISO currency code indicating the currency for the amount due (e.g., USD)."
                    },
                    "value": {
                      "anyOf": [
                        {
                          "type": "number"
                        },
                        {
                          "type": "null"
                        }
                      ],
                      "description": "The numeric monetary amount due, expressed as a decimal number with two decimal places."
                    }
                  },
                  "required": [
                    "currency",
                    "value"
                  ]
                },
                {
                  "type": "null"
                }
              ],
              "description": "The total amount owed by the buyer, structured as an object containing currency and numeric value."
            },
            "terms": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The payment terms specifying the time period within which payment is expected, expressed as text (e.g., 'Net 30 Days')."
            },
            "dueDate": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The date by which payment must be received, formatted as YYYY-MM-DD."
            }
          },
          "required": [
            "invoiceNumber",
            "invoiceDate",
            "purchaseOrderNumber",
            "salesOrderNumber",
            "amountDue",
            "terms",
            "dueDate"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Contains core invoice identification and payment information including numbers, dates, and amount due."
    },
    "seller": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "name": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The legal business name of the selling entity."
            },
            "address": {
              "anyOf": [
                {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "description": "An array of strings representing the seller's mailing address, with each array element containing a separate line of the address."
            }
          },
          "required": [
            "name",
            "address"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Contains information about the company or entity issuing the invoice and selling the goods or services."
    },
    "buyer": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "name": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The legal business name of the purchasing entity."
            },
            "address": {
              "anyOf": [
                {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "description": "An array of strings representing the buyer's billing address, with each array element containing a separate line of the address."
            }
          },
          "required": [
            "name",
            "address"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Contains information about the company or entity purchasing the goods or services and responsible for payment."
    },
    "shipTo": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "name": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The name of the company or organization receiving the shipment."
            },
            "attention": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The name of the specific person who should receive or be notified about the shipment."
            },
            "address": {
              "anyOf": [
                {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "description": "An array of strings representing the shipping address, with each array element containing a separate line of the address."
            }
          },
          "required": [
            "name",
            "attention",
            "address"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Contains information about the location and contact person where the goods should be delivered."
    },
    "lineItems": {
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "lineNumber": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "A sequential identifier for each line item, formatted as a zero-padded six-digit string (e.g., '000010')."
              },
              "manufacturerPartNumber": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "The manufacturer's unique part number or SKU for the product, consisting of alphanumeric characters and forward slashes."
              },
              "description": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "A text description of the product or service, including key specifications and features."
              },
              "quantityOrdered": {
                "anyOf": [
                  {
                    "type": "integer"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "The number of units of this item that were originally ordered by the buyer."
              },
              "quantityShipped": {
                "anyOf": [
                  {
                    "type": "integer"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "The actual number of units of this item that were shipped to fulfill the order."
              },
              "unitPrice": {
                "anyOf": [
                  {
                    "type": "integer"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "The price per individual unit of the item, expressed as a decimal number."
              },
              "extendedPrice": {
                "anyOf": [
                  {
                    "type": "integer"
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "The total price for this line item, calculated as quantity shipped multiplied by unit price."
              },
              "serialNumbers": {
                "anyOf": [
                  {
                    "type": "array",
                    "items": {
                      "type": "string"
                    }
                  },
                  {
                    "type": "null"
                  }
                ],
                "description": "An array of strings containing the serial numbers for individual units of this item. May be empty for items that do not have serial numbers."
              }
            },
            "required": [
              "lineNumber",
              "manufacturerPartNumber",
              "description",
              "quantityOrdered",
              "quantityShipped",
              "unitPrice",
              "extendedPrice",
              "serialNumbers"
            ]
          }
        },
        {
          "type": "null"
        }
      ],
      "description": "An array of objects representing individual products or services included in this invoice."
    },
    "totals": {
      "anyOf": [
        {
          "type": "object",
          "properties": {
            "subtotal": {
              "anyOf": [
                {
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The sum of all line item extended prices before taxes and shipping charges are applied."
            },
            "tax": {
              "anyOf": [
                {
                  "type": "number"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The total amount of taxes applied to this invoice, expressed as a decimal number."
            },
            "shipping": {
              "anyOf": [
                {
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The total shipping and handling charges for this invoice, expressed as a decimal number."
            },
            "total": {
              "anyOf": [
                {
                  "type": "number"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The final total amount due, calculated as subtotal plus tax plus shipping charges."
            },
            "currency": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "The three-letter ISO currency code indicating the currency for all monetary amounts in the totals section."
            }
          },
          "required": [
            "subtotal",
            "tax",
            "shipping",
            "total",
            "currency"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Contains the financial summary of the invoice including subtotal, taxes, shipping, and final total."
    }
  },
  "required": [
    "invoice",
    "seller",
    "buyer",
    "shipTo",
    "lineItems",
    "totals"
  ]
}
```
