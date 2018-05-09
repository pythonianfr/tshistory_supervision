from pathlib import Path
import pytest

from sqlalchemy import create_engine, MetaData

from pytest_sa_pg import db

from tshistory.schema import init, reset, delete_schema
from tshistory_supervision.tsio import TimeSerie

DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    meta = MetaData()
    with e.connect() as cn:
        reset(cn)
    delete_schema(e, 'tsh-automatic')
    delete_schema(e, 'tsh-manual')
    with e.connect() as cn:
        init(cn, meta)
        init(cn, meta, 'tsh-automatic')
        init(cn, meta, 'tsh-manual')
    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return TimeSerie()
