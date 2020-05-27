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
    return self.tsh.get_ts_marker(
        self.engine,
        name,
        revision_date=revision_date,
        from_value_date=from_value_date,
        to_value_date=to_value_date
    )


@extend(dbtimeseries)
def supervision_status(self, name):
    return self.tsh.supervision_status(
        self.engine,
        name
    )
