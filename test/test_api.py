import pandas as pd

from tshistory.testutil import assert_df


def test_multi_source_handcrafted(mapi):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    mapi.update('multi-local', series, 'test', manual=True)

    assert mapi.supervision_status('multi-local') == 'handcrafted'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", mapi.get('multi-local'))

    edited = series.copy()
    edited.iloc[1] = 42
    mapi.update('multi-local', edited, 'test')

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", mapi.get('multi-local'))

    # should be a noop
    mapi.update('multi-local', edited, 'test', manual=True)

    _, marker = mapi.edited('multi-local')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)


def test_multi_source_edited(mapi):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-1-1'), freq='D', periods=3)
    )
    mapi.update('multi-local2', series, 'test')
    assert mapi.supervision_status('multi-local') == 'supervised'

    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
""", mapi.get('multi-local2'))

    edited = series.copy()
    edited.iloc[1] = 42
    mapi.update('multi-local2', edited, 'test', manual=True)

    assert_df("""
2020-01-01     1.0
2020-01-02    42.0
2020-01-03     3.0
""", mapi.get('multi-local2'))

    _, marker = mapi.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02     True
2020-01-03    False
""", marker)

    # "upstream" fix
    mapi.update('multi-local2', edited, 'test')
    _, marker = mapi.edited('multi-local2')

    assert_df("""
2020-01-01    False
2020-01-02    False
2020-01-03    False
""", marker)
