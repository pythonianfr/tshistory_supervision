from pathlib import Path
from datetime import datetime
import pytest

import pandas as pd
import numpy as np

from tshistory.util import _set_cache
from tshistory.testutil import (
    assert_df,
    utcdt
)


def genserie(start, freq, repeat, initval=None, tz=None, name=None):
    if initval is None:
        values = range(repeat)
    else:
        values = initval * repeat
    return pd.Series(values,
                     name=name,
                     index=pd.date_range(start=start,
                                         freq=freq,
                                         periods=repeat,
                                         tz=tz))



def test_rename(engine, tsh):
    assert tsh.supervision_status(engine, 'rename-me') == 'unsupervised'
    tsh.update(engine, genserie(datetime(2010, 1, 1), 'D', 3),
               'rename-me', 'Babar')
    assert tsh.supervision_status(engine, 'rename-me') == 'unsupervised'
    tsh.update(engine, genserie(datetime(2010, 1, 2), 'D', 3),
               'rename-me', 'Babar', manual=True)
    assert tsh.supervision_status(engine, 'rename-me') == 'supervised'

    tsh.rename(engine, 'rename-me', 'renamed')

    assert tsh.get(engine, 'rename-me') is None
    assert tsh.get(engine, 'renamed') is not None
    assert tsh.upstream.get(engine, 'rename-me') is None
    assert tsh.upstream.get(engine, 'renamed') is not None


def test_manual_update(engine, tsh):
    # start testing manual overrides
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 5, [2.])
    ts_begin.loc['2010-01-04'] = -1
    tsh.update(engine, ts_begin, 'ts_mixte', 'test')

    assert tsh.supervision_status(engine, 'ts_mixte') == 'unsupervised'

    # -1 represents bogus upstream data
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
""", tsh.get(engine, 'ts_mixte'))

    # test marker for first inserstion
    _, marker = tsh.get_ts_marker(engine, 'ts_mixte')
    assert not marker.any()

    # refresh all the period + 1 extra data point
    ts_more = genserie(datetime(2010, 1, 2), 'D', 5, [2])
    ts_more.loc['2010-01-04'] = -1
    tsh.update(engine, ts_more, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
""", tsh.get(engine, 'ts_mixte'))

    # just append an extra data point
    # with no intersection with the previous ts
    ts_one_more = genserie(datetime(2010, 1, 7), 'D', 1, [2])
    tsh.update(engine, ts_one_more, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'ts_mixte'))
    assert tsh.supervision_status(engine, 'ts_mixte') == 'unsupervised'
    assert tsh.upstream.get(engine, 'ts_mixte') is None

    # edit the bogus upstream data: -1 -> 3
    # also edit the next value
    ts_manual = genserie(datetime(2010, 1, 4), 'D', 2, [3])
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    assert tsh.supervision_status(engine, 'ts_mixte') == 'supervised'
    upstream = tsh.upstream.get(engine, 'ts_mixte')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", upstream)

    ts, marker = tsh.get_ts_marker(engine, 'ts_mixte')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04     True
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts)

    # refetch upstream: the fixed value override must remain in place
    assert -1 == ts_begin['2010-01-04']
    tsh.update(engine, ts_begin, 'ts_mixte', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'ts_mixte'))

    # upstream provider fixed its bogus value: the manual override
    # should be replaced by the new provider value
    ts_begin_amend = ts_begin.copy()
    ts_begin_amend.iloc[3] = 2
    tsh.update(engine, ts_begin_amend, 'ts_mixte', 'test')
    ts, marker = tsh.get_ts_marker(engine, 'ts_mixte')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    2.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts)

    # another iterleaved editing session
    ts_edit = genserie(datetime(2010, 1, 4), 'D', 1, [2])
    tsh.update(engine, ts_edit, 'ts_mixte', 'test', manual=True)
    assert 2 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still
    ts, marker = tsh.get_ts_marker(engine, 'ts_mixte')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    # another iterleaved editing session
    drange = pd.date_range(start=datetime(2010, 1, 4), periods=1)
    ts_edit = pd.Series([4], index=drange)
    tsh.update(engine, ts_edit, 'ts_mixte', 'test', manual=True)
    assert 4 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    ts_auto_resend_the_same = pd.Series([2], index=drange)
    tsh.update(engine, ts_auto_resend_the_same, 'ts_mixte', 'test')
    assert 4 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    ts_auto_fix_value = pd.Series([7], index=drange)
    tsh.update(engine, ts_auto_fix_value, 'ts_mixte', 'test')
    assert 7 == tsh.get(engine, 'ts_mixte')['2010-01-04']  # still

    # test the marker logic
    # which helps put nice colour cues in the excel sheet
    # get_ts_marker returns a ts and its manual override mask
    # test we get a proper ts
    ts_auto, _ = tsh.get_ts_marker(engine, 'ts_mixte')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts_auto)

    ts_manual = genserie(datetime(2010, 1, 5), 'D', 2, [3])
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)

    ts_manual = genserie(datetime(2010, 1, 9), 'D', 1, [3])
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    tsh.update(engine, ts_auto, 'ts_mixte', 'test')

    upstream_fix = pd.Series([2.5], index=[datetime(2010, 1, 5)])
    tsh.update(engine, upstream_fix, 'ts_mixte', 'test')

    # we had three manual overrides, but upstream fixed one of its values
    tip_ts, tip_marker = tsh.get_ts_marker(engine, 'ts_mixte')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    3.0
""", tip_ts)

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05    False
2010-01-06     True
2010-01-07    False
2010-01-09     True
""", tip_marker)

    # just another override for the fun
    ts_manual.iloc[0] = 4
    tsh.update(engine, ts_manual, 'ts_mixte', 'test', manual=True)
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    7.0
2010-01-05    2.5
2010-01-06    3.0
2010-01-07    2.0
2010-01-09    4.0
""", tsh.get(engine, 'ts_mixte'))

    manual = tsh.get_overrides(engine, 'ts_mixte')
    assert_df("""
2010-01-06    3.0
2010-01-09    4.0
""", manual)

    with engine.begin() as cn:
        _set_cache(cn)
        revs = tsh._revisions(
            cn,
            'ts_mixte',
            qcallback=lambda q: q.where("cast(metadata ->> 'edited' as bool)")
        )
    assert [rid for rid, _ in revs] == [4, 6, 8, 9, 11]


def test_manual_replace(engine, tsh):
    # start testing manual overrides
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 5, [2.])
    ts_begin.loc['2010-01-04'] = -1
    tsh.replace(engine, ts_begin, 'mix_replace', 'test')

    assert tsh.supervision_status(engine, 'mix_replace') == 'unsupervised'

    # -1 represents bogus upstream data
    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
""", tsh.get(engine, 'mix_replace'))

    # test marker for first inserstion
    _, marker = tsh.get_ts_marker(engine, 'mix_replace')
    assert not marker.any()

    # refresh all the period + 1 extra data point
    ts_more = genserie(datetime(2010, 1, 1), 'D', 6, [2])
    ts_more.loc['2010-01-04'] = -1
    tsh.replace(engine, ts_more, 'mix_replace', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
""", tsh.get(engine, 'mix_replace'))

    # just append an extra data point
    # with no intersection with the previous ts
    ts_one_more = genserie(datetime(2010, 1, 7), 'D', 1, [2])
    tsh.update(engine, ts_one_more, 'mix_replace', 'test')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", tsh.get(engine, 'mix_replace'))
    assert tsh.supervision_status(engine, 'mix_replace') == 'unsupervised'
    assert tsh.upstream.get(engine, 'mix_replace') is None

    # edit the bogus upstream data: -1 -> 3
    # also edit the next value
    ts_manual = genserie(datetime(2010, 1, 4), 'D', 2, [3])
    tsh.update(engine, ts_manual, 'mix_replace', 'test', manual=True)
    assert tsh.supervision_status(engine, 'mix_replace') == 'supervised'
    upstream = tsh.upstream.get(engine, 'mix_replace')

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04   -1.0
2010-01-05    2.0
2010-01-06    2.0
2010-01-07    2.0
""", upstream)

    ts, marker = tsh.get_ts_marker(engine, 'mix_replace')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04     True
2010-01-05     True
2010-01-06    False
2010-01-07    False
""", marker)

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    2.0
2010-01-07    2.0
""", ts)


def test_strip(engine, tsh):
    ts = genserie(datetime(2019, 1, 1), 'D', 3)
    tsh.update(
        engine, ts, 'strip-unsupervised', 'test',
        insertion_date=utcdt(2019, 1, 1)
    )
    csid = tsh.changeset_at(engine, 'strip-unsupervised', utcdt(2019, 1, 1))
    tsh.strip(engine, 'strip-unsupervised', csid)

    ts = genserie(datetime(2019, 1, 1), 'D', 3)
    tsh.update(
        engine, ts, 'strip-handcrafted', 'test',
        manual=True,
        insertion_date=utcdt(2019, 1, 1)
    )
    csid = tsh.changeset_at(engine, 'strip-handcrafted', utcdt(2019, 1, 1))
    tsh.strip(engine, 'strip-handcrafted', csid)

    ts = genserie(datetime(2019, 1, 1), 'D', 3)
    tsh.update(
        engine, ts, 'strip-supervised', 'test',
        insertion_date=utcdt(2019, 1, 1)
    )
    ts = genserie(datetime(2019, 1, 2), 'D', 3)
    tsh.update(
        engine, ts, 'strip-supervised', 'test',
        manual=True,
        insertion_date=utcdt(2019, 1, 2)
    )

    with pytest.raises(ValueError) as err:
        csid = tsh.changeset_at(engine, 'strip-supervised', utcdt(2019, 1, 1))
        tsh.strip(engine, 'strip-supervised', csid)



def test_handcrafted(engine, tsh):
    ts_begin = genserie(datetime(2010, 1, 1), 'D', 10)
    tsh.update(engine, ts_begin, 'ts_only', 'test', manual=True)

    assert_df("""
2010-01-01    0.0
2010-01-02    1.0
2010-01-03    2.0
2010-01-04    3.0
2010-01-05    4.0
2010-01-06    5.0
2010-01-07    6.0
2010-01-08    7.0
2010-01-09    8.0
2010-01-10    9.0
""", tsh.get(engine, 'ts_only'))

    ts_slight_variation = ts_begin.copy()
    ts_slight_variation.iloc[3] = 0
    ts_slight_variation.iloc[6] = 0
    tsh.update(engine, ts_slight_variation, 'ts_only', 'test')

    assert_df("""
2010-01-01    0.0
2010-01-02    1.0
2010-01-03    2.0
2010-01-04    0.0
2010-01-05    4.0
2010-01-06    5.0
2010-01-07    0.0
2010-01-08    7.0
2010-01-09    8.0
2010-01-10    9.0
""", tsh.get(engine, 'ts_only'))

    # should be a noop
    tsh.update(engine, ts_slight_variation, 'ts_only', 'test', manual=True)
    _, marker = tsh.get_ts_marker(engine, 'ts_only')

    assert_df("""
2010-01-01    False
2010-01-02    False
2010-01-03    False
2010-01-04    False
2010-01-05    False
2010-01-06    False
2010-01-07    False
2010-01-08    False
2010-01-09    False
2010-01-10    False
""", marker)


def test_more_manual(engine, tsh):
    ts = genserie(datetime(2015, 1, 1), 'D', 5)
    tsh.update(engine, ts, 'ts_exp1', 'test')

    ts_man = genserie(datetime(2015, 1, 3), 'D', 3, -1)
    ts_man.iloc[-1] = np.nan
    # erasing of the laste value for the date 5/1/2015
    tsh.update(engine, ts_man, 'ts_exp1', 'test', manual=True)

    ts_get = tsh.get(engine, 'ts_exp1')

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
2015-01-03   -3.0
2015-01-04   -3.0
""", ts_get)

    ts_marker, marker = tsh.get_ts_marker(engine, 'ts_exp1')
    assert ts_marker.equals(ts_get)
    assert_df("""
2015-01-01    False
2015-01-02    False
2015-01-03     True
2015-01-04     True
2015-01-05     True
""", marker)


def test_before_first_insertion(engine, tsh):
    tsh.update(engine, genserie(datetime(2010, 1, 1), 'D', 11), 'ts_shtroumpf', 'test')

    # test get_marker with an unknown series vs a serie  displayed with
    # a revision date before the first insertion
    result = tsh.get_ts_marker(engine, 'unknown_ts')
    assert (None, None) == result

    result = tsh.get_ts_marker(engine, 'ts_shtroumpf', revision_date=datetime(1970, 1, 1))
    assert (None, None) == result


def test_na_and_delete(engine, tsh):
    ts_repushed = genserie(datetime(2010, 1, 1), 'D', 11)
    ts_repushed[0:3] = np.nan
    tsh.update(engine, ts_repushed, 'ts_repushed', 'test')
    diff = tsh.update(engine, ts_repushed, 'ts_repushed', 'test')
    assert len(diff) == 0


def test_exotic_name(engine, tsh):
    ts = genserie(datetime(2010, 1, 1), 'D', 11)
    tsh.update(engine, ts, 'ts-with_dash', 'test')
    tsh.get(engine, 'ts-with_dash')


def test_series_dtype(engine, tsh):
    tsh.update(engine,
               genserie(datetime(2015, 1, 1),
                        'D',
                        11).astype('str'),
               'error1',
               'test')

    with pytest.raises(Exception) as excinfo:
        tsh.update(engine,
                   genserie(datetime(2015, 1, 1),
                            'D',
                            11),
                   'error1',
                   'test')
    assert 'Type error when inserting error1, new type is float64, type in base is object' == str(excinfo.value)

    tsh.update(engine,
               genserie(datetime(2015, 1, 1),
                        'D',
                        11),
               'error2',
               'test')
    with pytest.raises(Exception) as excinfo:
        tsh.update(engine,
                   genserie(datetime(2015, 1, 1),
                            'D',
                            11).astype('str'),
                   'error2',
                   'test')
    assert 'Type error when inserting error2, new type is object, type in base is float64' == str(excinfo.value)


def test_serie_deletion(engine, tsh):

    def testit(tsh):
        ts = genserie(datetime(2018, 1, 10), 'H', 10)
        tsh.update(engine, ts, 'keepme', 'Babar')
        tsh.update(engine, ts, 'deleteme', 'Celeste')
        ts = genserie(datetime(2018, 1, 12), 'H', 10)
        tsh.update(engine, ts, 'keepme', 'Babar')
        tsh.update(engine, ts, 'deleteme', 'Celeste')

        with engine.begin() as cn:
            tsh.delete(cn, 'deleteme')

        assert not tsh.exists(engine, 'deleteme')
        tsh.update(engine, ts, 'deleteme', 'Celeste')

    testit(tsh)
    testit(tsh.upstream)


def test_create_empty_series(engine, tsh):
    ts = pd.Series(dtype='float64')
    tsh.update(engine, ts, 'empty', 'Babar')
    # did not fail :)
