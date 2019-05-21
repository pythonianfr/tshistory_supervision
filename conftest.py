from pathlib import Path
import pytest

from sqlalchemy import create_engine

from pytest_sa_pg import db

from tshistory.schema import tsschema

from tshistory_supervision.tsio import timeseries


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    sch1 = tsschema()
    sch2 = tsschema('tsh-automatic')
    e = create_engine(uri)
    sch1.create(e)
    sch2.create(e)
    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return timeseries()
