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
    def insert(self, cn, ts, name, author,
               metadata=None,
               _insertion_date=None, manual=False):
        if not self.exists(cn, name):
            # initial insert
            diff = super().insert(
                cn, ts, name, author,
                metadata=metadata,
                _insertion_date=_insertion_date
            )
            # the super call create the initial meta, let's complete it
            meta = self.metadata(cn, name)
            meta['supervision_status'] = 'handcrafted' if manual else 'unsupervised'
            self.update_metadata(cn, name, meta, internal=True)
            return diff

        supervision_status = self.supervision_status(cn, name)

        if supervision_status == 'unsupervised':
            if manual:
                # first supervised insert
                # let's take a copy of the current series state
                # into upstream and proceed forward
                current = self.get(cn, name)
                self.upstream.insert(
                    cn, current, name, author,
                    metadata=metadata,
                    _insertion_date=_insertion_date
                )
                # update supervision status
                meta = self.metadata(cn, name)
                meta['supervision_status'] = 'supervised'
                self.update_metadata(cn, name, meta, internal=True)
            # now insert what we got
            return super().insert(
                cn, ts, name, author,
                metadata=metadata,
                _insertion_date=_insertion_date
            )

        assert supervision_status in ('supervised', 'handcrafted')
        if manual:
            diff = ts
        else:
            # insert & compute diff over upstream
            diff = self.upstream.insert(
                cn, ts, name, author,
                metadata=metadata,
                _insertion_date=_insertion_date
            )

            if supervision_status == 'handcrafted':
                # update supervision status
                meta = self.metadata(cn, name)
                meta['supervision_status'] = 'supervised'
                self.update_metadata(cn, name, meta, internal=True)

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
                index=edited.index
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
        manual = self.diff(upstream, edited)

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
