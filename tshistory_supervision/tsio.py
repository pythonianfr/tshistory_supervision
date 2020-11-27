import pandas as pd
import numpy as np

from sqlalchemy import Column, Boolean, select, desc
from sqlalchemy.dialects.postgresql import BYTEA

from tshistory.util import tx, diff
from tshistory.tsio import timeseries as basets

from tshistory_supervision import api  # trigger registration


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
    metakeys = {
        'tzaware',
        'index_type',
        'index_dtype',
        'value_dtype',
        'value_type',
        # novelty
        'supervision_status'
    }
    supervision_states = ('unsupervised', 'supervised', 'handcrafted')

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.upstream = basets(namespace='{}-upstream'.format(self.namespace))

    def supervision_status(self, cn, name):
        meta = self.metadata(cn, name)
        if meta:
            return meta.get('supervision_status', 'unsupervised')
        return 'unsupervised'

    @tx
    def __supervise__(self, cn, ts, name, author,
                      metadata=None,
                      insertion_date=None, manual=False,
                      __supermethod__=None,
                      __upmethod__=None):

        if manual:
            if metadata is None:
                metadata = {}
            metadata['edited'] = True

        if not self.exists(cn, name):
            # initial insert
            series_diff = __supermethod__(
                cn, ts, name, author,
                metadata=metadata,
                insertion_date=insertion_date
            )
            if series_diff is None or not len(series_diff):
                return series_diff
            # the super call create the initial meta, let's complete it
            meta = self.metadata(cn, name)
            meta['supervision_status'] = 'handcrafted' if manual else 'unsupervised'
            self.update_metadata(cn, name, meta, internal=True)
            return series_diff

        supervision_status = self.supervision_status(cn, name)

        if supervision_status == 'unsupervised':
            if manual:
                # first supervised insert
                # let's take a copy of the current series state
                # into upstream and proceed forward
                current = self.get(cn, name)
                __upmethod__(
                    cn, current, name, author,
                    metadata=metadata,
                    insertion_date=insertion_date
                )
                # update supervision status
                meta = self.metadata(cn, name)
                meta['supervision_status'] = 'supervised'
                self.update_metadata(cn, name, meta, internal=True)
            # now insert what we got
            return __supermethod__(
                cn, ts, name, author,
                metadata=metadata,
                insertion_date=insertion_date
            )

        assert supervision_status in ('supervised', 'handcrafted')
        if manual:
            series_diff = ts
        else:
            # insert & compute diff over upstream
            series_diff = __upmethod__(
                cn, ts, name, author,
                metadata=metadata,
                insertion_date=insertion_date
            )

            if supervision_status == 'handcrafted':
                # update supervision status
                meta = self.metadata(cn, name)
                meta['supervision_status'] = 'supervised'
                self.update_metadata(cn, name, meta, internal=True)

            if series_diff is None:
                return

        # insert the diff over upstream or the manual edit into edited
        a = __supermethod__(
            cn, series_diff, name, author,
            metadata=metadata,
            insertion_date=insertion_date
        )
        return a

    @tx
    def update(self, cn, ts, name, author,
               metadata=None,
               insertion_date=None, manual=False):
        return self.__supervise__(
            cn, ts, name, author,
            metadata=metadata,
            insertion_date=insertion_date,
            manual=manual,
            __supermethod__=super().update,
            __upmethod__=self.upstream.update
        )

    @tx
    def replace(self, cn, ts, name, author,
               metadata=None,
               insertion_date=None, manual=False):
        return self.__supervise__(
            cn, ts, name, author,
            metadata=metadata,
            insertion_date=insertion_date,
            manual=manual,
            __supermethod__=super().replace,
            __upmethod__=self.upstream.replace
        )

    @tx
    def delete(self, cn, seriename):
        super().delete(cn, seriename)
        self.upstream.delete(cn, seriename)

    @tx
    def rename(self, cn, oldname, newname):
        super().rename(cn, oldname, newname)
        self.upstream.rename(cn, oldname, newname)

    @tx
    def strip(self, cn, name, csid):
        if self.supervision_status(cn, name) == 'supervised':
            raise ValueError(f'supervised series `{name}` cannot be striped')

        super().strip(cn, name, csid)

    # supervision specific API

    @tx
    def get_overrides(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
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
        manual = diff(upstream, edited)

        manual.name = name
        return manual

    @tx
    def get_ts_marker(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
        table = self._series_to_tablename(cn, name)
        if table is None:
            return None, None

        edited = self.get(
            cn, name,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            _keep_nans=True
        )
        if edited is None:
            # because of a revision_date
            return None, None

        supervision = self.supervision_status(cn, name)
        if supervision in ('unsupervised', 'handcrafted'):
            flags = pd.Series(
                [supervision == 'handcrafted'] * len(edited.index),
                index=edited.index,
                dtype=np.dtype('bool')
            )
            flags.name = name
            return edited.dropna(), flags

        upstreamtsh = self.upstream
        upstream = upstreamtsh.get(
            cn, name,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            _keep_nans=True
        )
        manual = diff(upstream, edited)

        unionindex = join_index(upstream, manual)
        if unionindex is None:
            # this means both series are empty
            return None, None

        mask_manual = pd.Series(
            [False] * len(unionindex),
            index=unionindex
        )
        if manual is not None:
            mask_manual[manual.index] = True
            mask_manual.name = name

        return edited.dropna(), mask_manual
