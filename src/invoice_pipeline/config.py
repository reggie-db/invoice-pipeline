import os

from databricks.sdk.runtime.dbutils_stub import dbutils

"""
Configuration helper for retrieving pipeline settings from multiple sources.

This module provides a unified interface for accessing configuration values
that may be stored in Databricks widgets, Spark configuration, or environment
variables. The lookup order ensures flexibility across different execution
contexts (notebooks, jobs, local development).

Lookup Order:
    1. Databricks widgets (dbutils.widgets.get)
    2. Spark configuration (spark.conf.get)
    3. Environment variables (os.environ)

Each source is checked with both the original key name and uppercase variant
to accommodate different naming conventions.
"""


# noinspection PyBroadException
def get(name: str):
    """
    Retrieve a configuration value from widgets, Spark conf, or environment.

    Searches multiple configuration sources in order of precedence to find
    the requested value. This allows the same code to run in notebooks
    (using widgets), DLT pipelines (using Spark conf), or local environments
    (using env vars).

    Args:
        name: Configuration key to look up. Both the original case and
              uppercase variants are checked.

    Returns:
        The configuration value as a string if found, None otherwise.

    Note:
        Exceptions from any source are silently caught to allow fallthrough
        to the next source. This is intentional to support environments
        where not all sources are available.
    """
    for upper in (False, True):
        key = name.upper() if upper else name
        # Try each configuration source in order of precedence
        for fn in (lambda n: dbutils.widgets.get(n), lambda n: spark.conf.get, lambda n: os.environ.get(n, None)):
            try:
                if (value := fn(key)) is not None:
                    return value
            except Exception:
                # Silently continue to next source on failure
                pass
    return None
