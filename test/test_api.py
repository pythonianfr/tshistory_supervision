import pandas as pd

from tshistory.testutil import assert_df


def test_multi_source_handcrafted(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsa.update('multi-local', series, 'test', manual=True)

    assert tsa.supervision_status('multi-local') == 'handcrafted'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsa.get('multi-local'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsa.update('multi-local', edited, 'test')

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsa.get('multi-local'))

    # should be a noop
    tsa.update('multi-local', edited, 'test', manual=True)

    _, marker = tsa.edited('multi-local')

    assert_df("""
2020-01-01    False
2020-01-02    False
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


def test_multi_source_edited(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    tsa.update('multi-local2', series, 'test')
    assert tsa.supervision_status('multi-local') == 'supervised'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", tsa.get('multi-local2'))

    edited = series.copy()
    edited.iloc[1] = 42
    tsa.update('multi-local2', edited, 'test', manual=True)

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", tsa.get('multi-local2'))

    _, marker = tsa.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)

    # "upstream" fix
    tsa.update('multi-local2', edited, 'test')
    _, marker = tsa.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)


def test_multi_source_edited(tsa1, tsa2):
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
