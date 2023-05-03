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
