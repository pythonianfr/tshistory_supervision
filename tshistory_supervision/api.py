from typing import Optional, Tuple

import pandas as pd

from tshistory.util import extend
from tshistory.api import (
    altsources,
    mainsource
)


@extend(mainsource)
def edited(self, name: str,
           revision_date: Optional[pd.Timestamp]=None,
           from_value_date: Optional[pd.Timestamp]=None,
           to_value_date: Optional[pd.Timestamp]=None) -> Tuple[pd.Series, pd.Series]:
    """
    Returns the base series and a second boolean series whose entries
    indicate if an override has been made or not.

    """
    with self.engine.begin() as cn:
        if self.tsh.exists(cn, name):
            return self.tsh.get_ts_marker(
                cn,
                name,
                revision_date=revision_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )

    return self.othersources.edited(
        name,
        revision_date,
        from_value_date,
        to_value_date
    )


@extend(altsources)
def edited(self,  # noqa: F811
           name,
           revision_date=None,
           from_value_date=None,
           to_value_date=None):

    source = self._findsourcefor(name)
    if source is None:
        return
    return source.tsa.edited(
        name,
        revision_date,
        from_value_date,
        to_value_date
    )


@extend(mainsource)
def supervision_status(self, name: str) -> str:
    """
    Returns the supervision status of a series.
    Possible values are `unsupervised`, `handcrafted` and `supervised`.
    """
    with self.engine.begin() as cn:
        if self.tsh.exists(cn, name):
            return self.tsh.supervision_status(
                cn,
                name
            )

    return self.othersources.supervision_status(name)


@extend(altsources)
def supervision_status(self, name):  # noqa: F811
    source = self._findsourcefor(name)
    if source is None:
        return
    return source.tsa.supervision_status(name)
