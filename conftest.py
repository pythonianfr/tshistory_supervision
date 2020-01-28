from pathlib import Path
import pytest

from sqlalchemy import create_engine

from pytest_sa_pg import db

from tshistory.schema import tsschema
from tshistory import api

from tshistory_supervision.tsio import timeseries


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    sch1 = tsschema()
    sch2 = tsschema('tsh-upstream')
    e = create_engine(uri)
    sch1.create(e)
    sch2.create(e)
    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return timeseries()


@pytest.fixture(scope='session')
def mapi(engine):
    tsschema('test-mapi').create(engine)
    tsschema('test-mapi-upstream').create(engine)
    tsschema('test-mapi-2').create(engine)
    tsschema('test-mapi-2-upstream').create(engine)

    return api.timeseries(
        str(engine.url),
        namespace='test-mapi',
        handler=timeseries,
        sources=[
            (str(engine.url), 'test-mapi-2')
        ]
    )
