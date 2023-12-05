import json

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
