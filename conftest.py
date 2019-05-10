from pathlib import Path
import pytest

from sqlalchemy import create_engine

from pytest_sa_pg import db

from tshistory.schema import (
    init_schemas,
    register_schema,
    reset_schemas,
    tsschema
)

from tshistory_supervision.tsio import timeseries


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    tsschema()
    tsschema('tsh-automatic')
    e = create_engine(uri)
    reset_schemas(e)
    init_schemas(e)
    yield e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return timeseries()
