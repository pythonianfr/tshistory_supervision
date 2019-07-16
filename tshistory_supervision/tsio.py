import pandas as pd
import numpy as np

from sqlalchemy import Column, Boolean, select, desc
from sqlalchemy.dialects.postgresql import BYTEA

from tshistory.util import tx
from tshistory.tsio import timeseries as basets


def join_index(ts1, ts2):
    if ts1 is None and ts2 is None:
        return None
    if ts1 is None:
        return ts2.index
    if ts2 is None:
        return ts1.index
    return ts1.index.union(ts2.index)


class timeseries(basets):
    """This class refines the base `tshistory.timeseries` by adding a
    specific workflow on top of it.

    We sometimes work with series that automatically fetched from some
    upstream source, and then eventually manually corrected (by an
    expert in the data domain)

    Say, one day, series X comes with a bogus value -1 for a given
    timestamp. The end user sees it and fixes it.

    But:

    * we don't want that the next upstream series fetch with the bogus
      value override the fix

    * however whenever upstream fixes the value (that is provides a
      new one) we want the manual override to be replaced by the new
      value.

    We can explain the workflow like with a traditional DVCS graph,
    with two branches: "upstream" and "edited".

    All upstream fetches go into the upstream branch (and thus are
    diffed against each other).

    The edited series receive all the non-empty differences
    resulting from inserting to the upsmtream series, and also all the
    manual entries.

    The manual editions are computed as a diffs between edited and
    upstream series.

    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.upstream = basets(namespace='{}-upstream'.format(self.namespace))

    @tx
    def insert(self, cn, ts, name, author,
               metadata=None,
               _insertion_date=None, manual=False):
        if manual:
            diff = ts
        else:
            # insert & compute diff over upstream
            diff = self.upstream.insert(
                cn, ts, name, author,
                metadata=metadata,
                _insertion_date=_insertion_date
            )
            if diff is None:
                return

        # insert the diff over upstream or the manual edit into edited
        a = super().insert(
            cn, diff, name, author,
            metadata=metadata,
            _insertion_date=_insertion_date
        )
        return a

    @tx
    def delete(self, cn, seriename):
        super().delete(cn, seriename)
        self.upstream.delete(cn, seriename)

    @tx
    def rename(self, cn, oldname, newname):
        super().rename(cn, oldname, newname)
        self.upstream.rename(cn, oldname, newname)

    # supervision specific API

    @tx
    def get_overrides(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
        upstreamtsh = self.upstream
        upstream = upstreamtsh.get(cn, name,
                           revision_date=revision_date,
                           from_value_date=from_value_date,
                           to_value_date=to_value_date,
                           _keep_nans=True)
        edited = self.get(cn, name,
                         revision_date=revision_date,
                         from_value_date=from_value_date,
                         to_value_date=to_value_date,
                         _keep_nans=True)
        manual = self.diff(upstream, edited)

        manual.name = name
        return manual

    @tx
    def get_ts_marker(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
        table = self._get_ts_table(cn, name)
        if table is None:
            return None, None

        upstreamtsh = self.upstream
        upstream = upstreamtsh.get(
            cn, name,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            _keep_nans=True
        )
        edited = self.get(
            cn, name,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            _keep_nans=True
        )
        manual = self.diff(upstream, edited)

        unionindex = join_index(upstream, manual)
        if unionindex is None:
            # this means both series are empty
            return None, None

        mask_manual = pd.Series([False] * len(unionindex), index=unionindex)
        if manual is not None:
            mask_manual[manual.index] = True
            mask_manual.name = name

        return edited.dropna(), mask_manual
