from pathlib import Path

import pytest
from sqlalchemy import create_engine
from pytest_sa_pg import db
import webtest

from tshistory.schema import tsschema
from tshistory import api
from tshistory.testutil import make_tsx
from tshistory.http import app

from tshistory_supervision import __version__
from tshistory_supervision.schema import supervision_schema
from tshistory_supervision import http
from tshistory_supervision.testutil import with_http_bridge
from tshistory_supervision.tsio import timeseries


DATADIR = Path(__file__).parent / 'test' / 'data'
DBURI = 'postgresql://localhost:5434/postgres'


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, 5434)
    e = create_engine(DBURI)
    supervision_schema().create(e)

    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return timeseries()


def make_api(engine, ns, sources={}):
    supervision_schema(ns).create(engine)

    for _, sns in sources.values():
        # create the remote sources with just
        # a base tshistory schema
        tsschema(sns).create(engine)

    return api.timeseries(
        str(engine.url),
        namespace=ns,
        handler=timeseries,
        sources=sources
    )


@pytest.fixture(scope='session')
def tsa(engine):
    return make_api(engine, 'test-api')


@pytest.fixture(scope='session')
def tsa1(engine):
    return make_api(
        engine,
        'test-api-2',
        {'remote': (str(engine.url), 'test-remote')}
    )


@pytest.fixture(scope='session')
def tsa2(engine):
    return make_api(
        engine,
        'test-remote'
    )


class WebTester(webtest.TestApp):

    def _check_status(self, status, res):
        try:
            super(WebTester, self)._check_status(status, res)
        except:
            print('ERRORS', res.errors)
            # raise <- default behaviour on 4xx is silly


@pytest.fixture(scope='session')
def client(engine):
    tsa = make_api(
        engine,
        'tsh',
        {'other': (DBURI, 'other')}
    )
    wsgi = app.make_app(tsa, http.supervision_httpapi)
    yield WebTester(wsgi)


def _initschema(engine):
    supervision_schema('tsh').create(engine)


tsx = make_tsx(
    'http://test-uri',
    _initschema,
    timeseries,
    http.supervision_httpapi,
    http.supervision_httpclient,
    with_http_bridge=with_http_bridge
)
