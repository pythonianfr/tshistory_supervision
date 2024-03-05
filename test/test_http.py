import json

import numpy as np
import pandas as pd

from tshistory import util
from tshistory.testutil import (
    assert_df,
    utcdt,
    genserie
)


def test_supervision_json(client):
    series = genserie(utcdt(2020, 1, 1), 'D', 3)
    client.patch('/series/state', params={
        'name': 'test-edited',
        'series': util.tojson(series),
        'author': 'Babar',
        'insertion_date': utcdt(2020, 1, 1, 10),
        'tzaware': util.tzaware_series(series)
    })

    series[-1] = 42
    client.patch('/series/state', params={
        'name': 'test-edited',
        'series': util.tojson(series),
        'author': 'Babar',
        'insertion_date': utcdt(2020, 1, 1, 11),
        'supervision': json.dumps(True),
        'tzaware': util.tzaware_series(series)
    })

    res = client.get('/series/supervision?name=test-edited')
    df = pd.read_json(res.text, orient='index')
    assert_df("""
                           series  markers
2020-01-01 00:00:00+00:00       0    False
2020-01-02 00:00:00+00:00       1    False
2020-01-03 00:00:00+00:00      42     True
""", df)

    res = client.get('/series/supervision?name=test-edited&format=tshpack')
    ts, marker = util.unpack_many_series(res.body)

    assert_df("""
2020-01-01 00:00:00+00:00     0.0
2020-01-02 00:00:00+00:00     1.0
2020-01-03 00:00:00+00:00    42.0
""", ts)

    assert_df("""
2020-01-01 00:00:00+00:00    False
2020-01-02 00:00:00+00:00    False
2020-01-03 00:00:00+00:00     True
""", marker)

    # NOTE: host me on some component doing integratin of formula + supervision
    # client.patch('/series/formula', params={
    #     'name': 'some-formula',
    #     'text': '(+ 3 (series "test-edited"))'
    # })
    # res = client.get('/series/supervision?name=some-formula')
    # assert res.status_code == 404
    # assert res.json == {'message': '`some-formula` is a formula'}


def test_supervision(tsx):
    series = genserie(utcdt(2020, 1, 1), 'D', 3)
    tsx.update('test-supervision', series, 'Babar')

    series[-1] = 42
    tsx.update('test-supervision', series, 'Babar', manual=True)

    ts, markers = tsx.edited('test-supervision')

    assert_df("""
2020-01-01 00:00:00+00:00     0.0
2020-01-02 00:00:00+00:00     1.0
2020-01-03 00:00:00+00:00    42.0
""", ts)

    assert_df("""
2020-01-01 00:00:00+00:00    False
2020-01-02 00:00:00+00:00    False
2020-01-03 00:00:00+00:00     True
""", markers)


def test_edited_by_horizon(client):
    ts = genserie(utcdt(2023, 1, 1), 'D', 60)
    client.patch('/series/state', params={
        'name': 'horizon',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2023, 1, 1),
        'tzaware': util.tzaware_series(ts),
        'supervision': json.dumps(False),
    })

    res = client.get('/series/supervision', params={
        'name': 'horizon',
        'horizon': (
            '(horizon #:date (date "2023-2-1")'
            '         #:offset 0'
            '         #:past (delta #:days -2) '
            '         #:future (delta #:days 1))'
        )
    })
    assert res.json == {
        '2023-01-30T00:00:00+00:00': {'markers': False, 'series': 29.0},
        '2023-01-31T00:00:00+00:00': {'markers': False, 'series': 30.0},
        '2023-02-01T00:00:00+00:00': {'markers': False, 'series': 31.0},
        '2023-02-02T00:00:00+00:00': {'markers': False, 'series': 32.0}
    }
    df = pd.read_json(res.text, orient='index')
    ts = df['series']
    ts[-1] = 42

    client.patch('/series/state', params={
        'name': 'horizon',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2023, 1, 1, 1),
        'tzaware': util.tzaware_series(ts),
        'supervision': json.dumps(True),
    })

    res = client.get('/series/supervision', params={
        'name': 'horizon',
        'horizon': (
            '(horizon #:date (date "2023-2-1")'
            '         #:offset 0'
            '         #:past (delta #:days -2) '
            '         #:future (delta #:days 1))'
        ),
        'format': 'tshpack'
    })
    ts, marker = util.unpack_many_series(res.body)

    assert_df("""
2023-01-30 00:00:00+00:00    29.0
2023-01-31 00:00:00+00:00    30.0
2023-02-01 00:00:00+00:00    31.0
2023-02-02 00:00:00+00:00    42.0
""", ts)

    assert_df("""
2023-01-30 00:00:00+00:00    False
2023-01-31 00:00:00+00:00    False
2023-02-01 00:00:00+00:00    False
2023-02-02 00:00:00+00:00     True
""", marker)


def test_edited_with_timezone(client):
    ts = genserie(utcdt(2023, 1, 1), 'D', 5)
    client.patch('/series/state', params={
        'name': 'withtz',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2023, 1, 1),
        'tzaware': util.tzaware_series(ts),
        'supervision': json.dumps(False),
    })
    res = client.get('/series/supervision', params={
        'name': 'withtz',
        'tzone': 'Europe/Paris'
    })
    assert res.json == {
        '2023-01-01T01:00:00+01:00': {'markers': False, 'series': 0.0},
        '2023-01-02T01:00:00+01:00': {'markers': False, 'series': 1.0},
        '2023-01-03T01:00:00+01:00': {'markers': False, 'series': 2.0},
        '2023-01-04T01:00:00+01:00': {'markers': False, 'series': 3.0},
        '2023-01-05T01:00:00+01:00': {'markers': False, 'series': 4.0}
    }

    # put a nan
    ts[2] = np.nan
    client.patch('/series/state', params={
        'name': 'withtz',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2023, 1, 2),
        'tzaware': util.tzaware_series(ts),
        'supervision': json.dumps(False),
    })

    res = client.get('/series/supervision', params={
        'name': 'withtz',
        'tzone': 'Europe/Paris',
        '_keep_nans': json.dumps(True)
    })
    assert 'null' in res.text


def test_infer_freq_http(client):
    ts = pd.Series(
        [1, 2, 3, 4, 6],
        index=[
            pd.Timestamp('2024-01-01', tz='UTC'),
            pd.Timestamp('2024-01-02', tz='UTC'),
            pd.Timestamp('2024-01-03', tz='UTC'),
            pd.Timestamp('2024-01-04', tz='UTC'),
            # hole in here
            pd.Timestamp('2024-01-06', tz='UTC'),
        ]
    )
    client.patch('/series/state', params={
        'name': 'series_holes_tz_aware',
        'series': util.tojson(ts),
        'author': 'Babar',
        'tzaware': util.tzaware_series(ts),
        'supervision': json.dumps(False),
    })
    res = client.get('/series/supervision', params={
        'name': 'series_holes_tz_aware',
        'tzone': 'Europe/Paris',
        'format': 'tshpack',
    })
    series, markers = util.unpack_many_series(res.body)
    assert 5 == len(series) == len(markers)

    res = client.get('/series/supervision', params={
        'name': 'series_holes_tz_aware',
        'tzone': 'Europe/Paris',
        'format': 'tshpack',
        'inferred_freq': json.dumps(True),
    })
    series, markers = util.unpack_many_series(res.body)
    assert 6 == len(series) == len(markers)

    res = client.get('/series/supervision', params={
        'name': 'series_holes_tz_aware',
        'tzone': 'Europe/Paris',
        'format': 'tshpack',
        'inferred_freq': json.dumps(False),
    })
    series, markers = util.unpack_many_series(res.body)
    # There should be only 5 values, like the call without
    # the "inferred_freq" params
    assert 6 == len(series) == len(markers)
