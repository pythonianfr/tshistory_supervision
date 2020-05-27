from tshistory.util import extend
from tshistory.api import (
    altsources,
    dbtimeseries
)


@extend(dbtimeseries)
def edited(self, name,
           revision_date=None,
           from_value_date=None,
           to_value_date=None):
    """
    returns the base series and a second boolean series whose entries
    indicate if an override has been made or not
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
def edited(self, name,
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


@extend(dbtimeseries)
def supervision_status(self, name):
    with self.engine.begin() as cn:
        if self.tsh.exists(cn, name):
            return self.tsh.supervision_status(
                cn,
                name
            )

    return self.othersources.supervision_status(name)


@extend(altsources)
def supervision_status(self, name):
    source = self._findsourcefor(name)
    if source is None:
        return
    return source.tsa.supervision_status(name)
