import contextlib
import os

from databricks.sdk.runtime.dbutils_stub import dbutils
from pyspark.sql import SparkSession

"""
Configuration helper for retrieving pipeline settings from multiple sources.

Provides a unified interface for accessing configuration values stored in
Spark configuration, Databricks widgets, or environment variables.

Lookup Order:
    1. Spark configuration (spark.conf.get)
    2. Databricks widgets (dbutils.widgets.get)
    3. Environment variables (os.environ)

Each source is checked with both the original key name and uppercase variant.
"""

_UNSET = object()



def get(
    name: str,
    default_value: str | None = _UNSET,
    spark: SparkSession | None = None,
    dbutils_instance: dbutils | None = None,
) -> str | None:
    """
    Retrieve a configuration value from Spark conf, widgets, or environment.

    Args:
        name: Configuration key to look up. Both original case and uppercase are checked.
        default_value: Value to return if key is not found. Raises KeyError if unset.
        spark: Optional SparkSession instance. Created lazily if not provided.
        dbutils_instance: Optional dbutils instance. Resolved lazily if not provided.

    Returns:
        Configuration value as a string, or default_value if not found.

    Raises:
        KeyError: If key is not found and no default_value is provided.
    """

    def _spark() -> SparkSession:
        nonlocal spark
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        return spark

    def _dbutils() -> dbutils:
        nonlocal dbutils_instance
        if dbutils_instance is None:
            with contextlib.suppress(Exception):
                from IPython.core.getipython import get_ipython

                if ip := get_ipython():
                    dbutils_instance = ip.user_ns["dbutils"]
        if dbutils_instance is None:
            from pyspark.dbutils import DBUtils

            dbutils_instance = DBUtils(_spark())
        return dbutils_instance

    readers = [
        lambda n: _spark().conf.get(n, None),
        lambda n: _dbutils().widgets.get(n),
        lambda n: os.environ.get(n, None),
    ]

    for upper in (False, True):
        key = name.upper() if upper else name
        for reader in readers:
            with contextlib.suppress(Exception):
                if (value := reader(key)) is not None:
                    return value

    if default_value is _UNSET:
        raise KeyError(name)

    return default_value
