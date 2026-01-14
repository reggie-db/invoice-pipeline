import os

from databricks.sdk.runtime.dbutils_stub import dbutils


# noinspection PyBroadException
def get(name: str):
    for upper in (False, True):
        key = name.upper() if upper else name
        for fn in (lambda n: dbutils.widgets.get(n), lambda n: spark.conf.get, lambda n: os.environ.get(n, None)):
            try:
                if (value := fn(key)) is not None:
                    return value
            except Exception:
                pass
    return None
