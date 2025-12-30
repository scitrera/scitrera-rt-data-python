from __future__ import annotations

from typing import SupportsInt, SupportsFloat, Any

import pandas as pd
from pandas._libs.missing import NAType  # TODO: way to get this without going through pandas internals?

Timestamp = pd.Timestamp
_nan = float('nan')


def default_encoder(o):
    if isinstance(o, SupportsFloat):  # this handles numpy and other similar float types (e.g. np.float64)
        return float(o)
    elif isinstance(o, SupportsInt):  # this handles numpy and other similar int types (e.g. np.int16)
        return int(o)
    elif isinstance(o, Timestamp):  # TODO: implement timestamps in a more sustainable way?
        #                                   [but that would have to vary across serialization methods]
        return o.value  # int time in ns since unix epoch UTC
    elif isinstance(o, NAType):  # handle NAType from pandas (occurs e.g., for arrow dataframes...)
        return _nan

    # try to support pydantic models if possible
    try:
        # noinspection PyUnresolvedReferences
        from pydantic import BaseModel
        if isinstance(o, BaseModel):
            return o.model_dump(mode='json')
    except ImportError:
        pass

    raise TypeError(f'object type not supported for encoding')
