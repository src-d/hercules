from datetime import datetime
from numbers import Number
from typing import TYPE_CHECKING

import numpy

if TYPE_CHECKING:
    from pandas import Timestamp


def floor_datetime(dt: datetime, duration: float) -> datetime:
    return datetime.fromtimestamp(dt.timestamp() - dt.timestamp() % duration)


def default_json(x):
    if hasattr(x, "tolist"):
        return x.tolist()
    if hasattr(x, "isoformat"):
        return x.isoformat()
    return x


def parse_date(text: None, default: 'Timestamp') -> 'Timestamp':
    if not text:
        return default
    from dateutil.parser import parse

    return parse(text)


def _format_number(n: Number) -> str:
    if n == 0:
        return "0"
    power = int(numpy.log10(abs(n)))
    if power >= 6:
        n = n / 1000000
        if n >= 10:
            n = str(int(n))
        else:
            n = "%.1f" % n
            if n.endswith("0"):
                n = n[:-2]
        suffix = "M"
    elif power >= 3:
        n = n / 1000
        if n >= 10:
            n = str(int(n))
        else:
            n = "%.1f" % n
            if n.endswith("0"):
                n = n[:-2]
        suffix = "K"
    else:
        n = str(n)
        suffix = ""
    return n + suffix


def import_pandas():
    import pandas

    try:
        from pandas.plotting import register_matplotlib_converters

        register_matplotlib_converters()
    except ImportError:
        pass
    return pandas
