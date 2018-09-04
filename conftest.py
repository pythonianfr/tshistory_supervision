from pathlib import Path
import pytest

from sqlalchemy import create_engine, MetaData

from pytest_sa_pg import db

from tshistory_supervision.schema import init, reset
from tshistory_supervision.tsio import TimeSerie

DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    meta = MetaData()
    reset(e)
    init(e, meta)
    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return TimeSerie()
