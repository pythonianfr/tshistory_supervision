import pandas as pd
import numpy as np

from sqlalchemy import Column, Boolean, select, desc
from sqlalchemy.dialects.postgresql import BYTEA

from tshistory.tsio import TimeSerie as BaseTS


def join_index(ts1, ts2):
    if ts1 is None and ts2 is None:
        return None
    if ts1 is None:
        return ts2.index
    if ts2 is None:
        return ts1.index
    return ts1.index.union(ts2.index)


class TimeSerie(BaseTS):
    """This class refines the base `tshistory.TimeSerie` by adding a
    specific workflow on top of it.

    There are two kinds of series : automatically fetched, and
    manually imposed.  The idea is that some scrapper fetches the
    automatic series, and endusers sometimes override values from the
    automatic series.

    Say, one day, Serie X comes with a bogus value -1 for a given
    timestamp. The end user sees it and fixes it.

    But:

    * we don't want that the next automatic serie fetch with the bogus
      value override the fix

    * however whenever upstream fixes the value (that is provides a
      new one) we want the manual override to be replaced by the new
      value.

    We can explain the workflow like with a traditional DVCS graph,
    with two branches: "automatic" and "synthetic".

    All automatic fetches go into the automatic branch (and thus are
    diffed against each other).

    The synthetic series receive all the non-empty differences
    resulting from inserting to the automatic series, and also
    all the manual entries.

    The manual editions can be computed as a diff between synthetic
    and automatic series.
    """

    def insert(self, cn, ts, name, author, _insertion_date=None, manual=False):
        if manual:
            basetsh = BaseTS(namespace='manual')
            # insert & compute diff over synthetic
            diff = basetsh.insert(
                cn, ts, name, author, _insertion_date=_insertion_date
            )

            if diff is None:
                return
            diff = ts

        else:
            # insert into synthetic & compute diff over automatic
            basetsh = BaseTS(namespace='automatic')
            diff = basetsh.insert(
                cn, ts, name, author,
                _insertion_date=_insertion_date
            )
            if diff is None:
                return

        # insert the diff over automatic into synthetic
        a = super().insert(
            cn, diff, name, author,
            _insertion_date=_insertion_date
        )
        return a

    # supervision specific API

    def get_ts_marker(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
        table = self._get_ts_table(cn, name)
        if table is None:
            return None, None

        autotsh = BaseTS(namespace='automatic')
        auto = autotsh.get(cn, name,
                           revision_date=revision_date,
                           from_value_date=from_value_date,
                           to_value_date=to_value_date,
                           _keep_nans=True)
        synth = self.get(cn, name,
                         revision_date=revision_date,
                         from_value_date=from_value_date,
                         to_value_date=to_value_date,
                         _keep_nans=True)
        manual = self.diff(auto, synth)

        unionindex = join_index(auto, manual)
        if unionindex is None:
            # this means both series are empty
            return None, None

        mask_manual = pd.Series([False], index=unionindex)
        if manual is not None:
            mask_manual[manual.index] = True
            mask_manual.name = name

        ts = self.get(cn, name,
                      revision_date=revision_date,
                      from_value_date=from_value_date,
                      to_value_date=to_value_date)
        ts.name = name
        return ts, mask_manual
