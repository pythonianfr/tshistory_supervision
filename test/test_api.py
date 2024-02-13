import numpy as np
import pandas as pd

from tshistory.testutil import assert_df


def test_multi_source_handcrafted(tsx):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsx.update('multi-local', series, 'test', manual=True)

    assert tsx.supervision_status('multi-local') == 'handcrafted'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsx.get('multi-local'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsx.update('multi-local', edited, 'test')

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsx.get('multi-local'))

    # should be a noop
    tsx.update('multi-local', edited, 'test', manual=True)

    _, marker = tsx.edited('multi-local')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)

    edited = series.copy()
    edited.iloc[1] = np.NaN
    tsx.update('multi-local', edited, 'test', manual=True)

    ts, marker = tsx.edited('multi-local', _keep_nans=True)
    assert_df("""
2020-01-01    1.0
2020-01-02    NaN
2020-01-03    3.0
""", ts)
    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)

    ts, marker = tsx.edited('multi-local', _keep_nans=False)
    assert_df("""
2020-01-01    1.0
2020-01-03    3.0
""", ts)
    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)


def test_multi_source_handcrafted_federated(tsa1, tsa2):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsa2.update('multi-local', series, 'test', manual=True)

    assert tsa1.supervision_status('multi-local') == 'handcrafted'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsa1.get('multi-local'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsa2.update('multi-local', edited, 'test')

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsa1.get('multi-local'))

    # should be a noop
    tsa2.update('multi-local', edited, 'test', manual=True)

    _, marker = tsa1.edited('multi-local')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)


def test_multi_source_edited(tsx):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsx.update('multi-local2', series, 'test')

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsx.get('multi-local2'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsx.update('multi-local2', edited, 'test', manual=True)

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsx.get('multi-local2'))

    _, marker = tsx.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)

    # "upstream" fix
    tsx.update('multi-local2', edited, 'test')
    _, marker = tsx.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)


def test_multi_source_edited2(tsa1, tsa2):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsa2.update('multi-local2', series, 'test')
    assert tsa1.supervision_status('multi-local') == 'supervised'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsa1.get('multi-local2'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsa2.update('multi-local2', edited, 'test', manual=True)

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsa1.get('multi-local2'))

    _, marker = tsa1.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)

    # "upstream" fix
    tsa2.update('multi-local2', edited, 'test')
    _, marker = tsa1.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)


def test_infer_freq(tsx):
    ts = pd.Series(
        [1, 2, 3, 4, 6],
        index=[
            pd.Timestamp('2024-01-01'),
            pd.Timestamp('2024-01-02'),
            pd.Timestamp('2024-01-03'),
            pd.Timestamp('2024-01-04'),
            pd.Timestamp('2024-01-06'),
        ]
    )
    tsx.update('series_with_holes', ts, 'test')

    ts, markers = tsx.edited('series_with_holes')
    assert len(ts) == 5

    ts, markers = tsx.edited('series_with_holes', inferred_freq=True)

    assert_df("""
2024-01-01    1.0
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
2024-01-06    6.0
""", ts)

    assert_df("""
2024-01-01    False
2024-01-02    False
2024-01-03    False
2024-01-04    False
2024-01-05    False
2024-01-06    False
""", markers)

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        to_value_date=pd.Timestamp('2024-01-08 12:00:00')
    )
    assert_df("""
2024-01-01    1.0
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
2024-01-06    6.0
2024-01-07    NaN
2024-01-08    NaN
""", ts)
    assert len(markers) == 8

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        to_value_date=pd.Timestamp('2024-01-05 12:00:00')
    )
    assert_df("""
2024-01-01    1.0
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
""", ts)
    assert len(markers) == 5

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        from_value_date=pd.Timestamp('2023-12-30 18:00:00'),
    )
    assert_df("""
2023-12-31    NaN
2024-01-01    1.0
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
2024-01-06    6.0
""", ts)
    assert len(markers) == 7

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        from_value_date=pd.Timestamp('2024-01-01 18:00:00'),
    )
    assert_df("""
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
2024-01-06    6.0
""", ts)
    assert len(markers) == 5

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        from_value_date=pd.Timestamp('2023-12-30 18:00:00'),
        to_value_date=pd.Timestamp('2024-01-07 12:00:00')
    )
    assert_df("""
2023-12-31    NaN
2024-01-01    1.0
2024-01-02    2.0
2024-01-03    3.0
2024-01-04    4.0
2024-01-05    NaN
2024-01-06    6.0
2024-01-07    NaN
""", ts)

    assert len(markers) == 8

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        from_value_date=pd.Timestamp('2024-01-02 12:00:00'),
        to_value_date=pd.Timestamp('2024-01-05 12:00:00')
    )
    assert_df("""
2024-01-03    3.0
2024-01-04    4.0
""", ts)

    assert len(markers) == 2

    ts, markers = tsx.edited(
        'series_with_holes',
        inferred_freq=True,
        from_value_date=pd.Timestamp('2024-01-04 12:00:00'),
        to_value_date=pd.Timestamp('2024-01-07 12:00:00')
    )
    assert_df("""
2024-01-06    6.0
""", ts)

    assert len(markers) == 1


def test_infer_freq_tz(tsx):
    """Since we build a pseudo index based
    both on request bounds and the series index
    we make sure that the tz-status are correctly processed"""

    ts = pd.Series(
        [1, 2, 3, 4, 6],
        index=[
            pd.Timestamp('2024-01-01'),
            pd.Timestamp('2024-01-02'),
            pd.Timestamp('2024-01-03'),
            pd.Timestamp('2024-01-04'),
            pd.Timestamp('2024-01-06'),
        ]
    )
    tsx.update('series_with_holes_naive', ts, 'test')

    ts = pd.Series(
        [1, 2, 3, 4, 6],
        index=[
            pd.Timestamp('2024-01-01', tz='UTC'),
            pd.Timestamp('2024-01-02', tz='UTC'),
            pd.Timestamp('2024-01-03', tz='UTC'),
            pd.Timestamp('2024-01-04', tz='UTC'),
            pd.Timestamp('2024-01-06', tz='UTC'),
        ]
    )
    tsx.update('series_with_holes_tz_aware', ts, 'test')

    from_naive = pd.Timestamp('2024-01-01 12:00:00')
    to_naive = pd.Timestamp('2024-01-07 12:00:00')

    from_tz_aware = pd.Timestamp('2024-01-01 12:00:00', tz='CET')
    to_tz_aware = pd.Timestamp('2024-01-07 12:00:00', tz='CET')

    assert len(
        tsx.edited(
            'series_with_holes_naive',
            from_value_date=from_tz_aware,
            to_value_date=to_tz_aware,
            inferred_freq=True,
        )[0]
    ) == 6

    assert len(
        tsx.edited(
            'series_with_holes_tz_aware',
            from_value_date=from_tz_aware,
            to_value_date=to_tz_aware,
            inferred_freq=True,
        )[0]
    ) == 6

    assert len(
        tsx.edited(
            'series_with_holes_tz_aware',
            from_value_date=from_naive,
            to_value_date=to_naive,
            inferred_freq=True,
        )[0]
    ) == 6
