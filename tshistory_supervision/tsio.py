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
    with two branches: "automatic" and "manual".

    All automatic fetches go into the automatic branch (and thus are
    diffed against each other).

    The manual series are rooted from the (current) top of the
    automatic series, but live in their own branch.

    As soon as a new automatic serie is inserted, it is also *merged*
    on top of the manual branch.

    Hence, the manual branch contains all the series + edits, and
    whenever an automatic serie fixes an old error, it is merged into
    the series + edits branch, which contains the correct synthesis.

    The concrete implementation is not Hg/Git since it uses a
    single-parent model. We use filtering and a modicum of ad-hoc
    transformations to achieve the desired effect.

    """
    _saveme = None
    _snapshot_interval = 100

    def insert(self, cn, ts, name, author=None,
               _insertion_date=None,
               extra_scalars={}):
        initial_insertion = not self.exists(cn, name)
        if initial_insertion and not extra_scalars.get('manual', False):
            if ts.isnull().all():
                return None
            ts = ts[~ts.isnull()]
            self._saveme = {'autosnapshot': ts}
        diff = super(TimeSerie, self).insert(cn, ts, name, author=author,
                                             _insertion_date=_insertion_date,
                                             extra_scalars=extra_scalars)

        return diff

    # log

    def log(self, cn, *args, **kw):
        logs = super(TimeSerie, self).log(cn, *args, **kw)

        for rev in logs:
            rev['manual'] = attrs = {}
            for name in rev['names']:
                attrs[name] = self._manual_value(cn, rev['rev'], name)

        return logs

    def _manual_value(self, cn, csetid, seriename):
        table = self._table_definition_for(seriename)
        sql = select([table.c.manual]).where(table.c.csid == csetid)
        return cn.execute(sql).scalar()

    # /log

    def _table_definition_for(self, tablename):
        tdef = super(TimeSerie, self)._table_definition_for(tablename)
        tdef.append_column(Column('manual', Boolean, default=False, index=True))
        tdef.append_column(Column('autosnapshot', BYTEA))
        return tdef

    def _complete_insertion_value(self, value, extra_scalars):
        if extra_scalars:
            value.update(extra_scalars)

        if self._saveme is not None:
            value.update({k: self._serialize(v)
                          for k, v in self._saveme.items()}
            )
            self._saveme = None

    def _latest_item(self, cn, table, column):
        # fetch the top-level things (e.g. snapshot, autosnapshot)
        sql = select([table.c[column]]
        ).order_by(desc(table.c.id)
        ).limit(1)
        return cn.execute(sql).scalar()

    def _purge_snapshot_at(self, cn, table, diffid):
        cn.execute(
            table.update(
            ).where(table.c.id == diffid
            ).values(snapshot=None, autosnapshot=None)
        )

    def _compute_diff_and_newsnapshot(self, cn, table, newts, manual=False):
        auto = self._latest_item(cn, table, 'autosnapshot')
        if auto is None:
            auto = self._build_snapshot_upto(cn, table,
                                             [lambda _, table: table.c.manual == False])
        else:
            auto = self._deserialize(auto, table.name)
        synthetic = self._build_snapshot_upto(cn, table)

        self._validate_type(auto, newts, table.name)
        self._validate_type(synthetic, newts, table.name)
        # this is the diff between our computed parent
        diff = self._compute_diff(synthetic if manual else auto,
                                  newts)

        if len(diff) == 0:
            return None, None

        # maintain the auto snapshot
        self._saveme = {
            'autosnapshot': auto if manual else self._apply_diff(auto, diff)
        }

        return diff, self._apply_diff(synthetic, diff)

    # we still need a full-blown history reconstruction routine
    # for arbitrary revision_dates

    def _build_snapshots_upto(self, cn, table, qfilter,
                              from_value_date=None, to_value_date=None):
        snapid, synthsnap = self._find_snapshot(cn, table, qfilter,
                                                from_value_date=from_value_date,
                                                to_value_date=to_value_date)
        auto_snapid, autosnap = self._find_snapshot(cn, table, qfilter,
                                                    column='autosnapshot',
                                                    from_value_date=from_value_date,
                                                    to_value_date=to_value_date)
        if snapid is None:
            return None, None  # yes, we can be asked fancy revision dates

        if auto_snapid is not None:
            assert snapid == auto_snapid

        cset = self.schema.changeset
        sql = select([table.c.id,
                      table.c.diff,
                      table.c.parent,
                      table.c.manual,
                      cset.c.insertion_date]
        ).order_by(table.c.id
        ).where(table.c.csid == cset.c.id
        ).where(table.c.id > snapid)

        for filtercb in qfilter:
            sql = sql.where(filtercb(cset, table))

        alldiffs = pd.read_sql(sql, cn)

        if len(alldiffs) == 0:
            manual_ts = self._compute_diff(autosnap, synthsnap)
            return autosnap, manual_ts

        # rebuild automatic & manual residual starting
        # from the last known (synthetic) state
        synth_ts = synthsnap
        auto_ts = autosnap if autosnap is not None else pd.Series()

        for _, row in alldiffs.iterrows():
            diff = self._deserialize(row['diff'], table.name)

            if row['manual']:
                synth_ts = self._apply_diff(synth_ts, diff)
            else:
                auto_ts = self._apply_diff(auto_ts, diff)
                # merging auto into manual
                # we erase all the elements that have been edited
                # by the auto diff
                synth_ts = synth_ts[~synth_ts.index.isin(diff.index)]

        manual_ts = self._compute_diff(auto_ts, synth_ts)
        return auto_ts, manual_ts

    def _onthefly(self, cn, table, revision_date,
                  from_value_date=None, to_value_date=None):
        qfilter = []
        if revision_date:
            qfilter.append(lambda cset, _: cset.c.insertion_date <= revision_date)
        return self._build_snapshots_upto(cn, table, qfilter,
                                          from_value_date=from_value_date,
                                          to_value_date=to_value_date)

    # public API redefinition

    def get(self, cn, name, revision_date=None):
        table = self._get_ts_table(cn, name)
        if table is None:
            return

        if revision_date:
            auto, residualmanual = self._onthefly(cn, table, revision_date)
            ts = self._apply_diff(auto, residualmanual)
        else:
            # fetch the top-level snapshot
            synthetic = self._latest_item(cn, table, 'snapshot')
            if synthetic is None: # head just got chopped
                ts = self._build_snapshot_upto(cn, table)
            else:
                ts = self._deserialize(synthetic, name)

        if ts is not None:
            ts.name = name
            ts = ts[~ts.isnull()]
        return ts

    # updated to account for the possibility of stripping changesets
    # of their series diffs

    def _diff(self, cn, csetid, name):
        table = self._get_ts_table(cn, name)
        cset = self.schema.changeset

        def filtercset(sql):
            return sql.where(table.c.csid == cset.c.id
            ).where(cset.c.id == csetid)

        sql = filtercset(select([table.c.id]))
        tsid = cn.execute(sql).scalar()

        # that guy was stripped
        if tsid is None:
            return pd.Series()

        if tsid == 1:
            sql = select([table.c.snapshot])
        else:
            sql = select([table.c.diff])
        sql = filtercset(sql)

        return self._deserialize(cn.execute(sql).scalar(), name)

    # supervision specific API

    def get_ts_marker(self, cn, name, revision_date=None,
                      from_value_date=None, to_value_date=None):
        table = self._get_ts_table(cn, name)
        if table is None:
            return None, None

        auto, manual = self._onthefly(cn, table, revision_date,
                                      from_value_date=from_value_date,
                                      to_value_date=to_value_date)
        unionindex = join_index(auto, manual)
        if unionindex is None:
            # this means both series are empty
            return None, None

        mask_manual = pd.Series([False], index=unionindex)
        if manual is not None:
            mask_manual[manual.index] = True
            mask_manual.name = name

        ts = self._apply_diff(auto, manual)
        ts = ts[~ts.isnull()]
        ts.name = name
        return ts, mask_manual
