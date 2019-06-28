import click

from sqlalchemy import create_engine
from tshistory.util import find_dburi

from tshistory_supervision.tsio import timeseries


@click.command(name='migrate-supervision')
@click.argument('dburi')
@click.option('--namespace', default='tsh')
def migrate_supervision(dburi, skip_schema=False, namespace='tsh'):
    engine = create_engine(find_dburi(dburi))

    with engine.begin() as cn:
        cn.execute(
            f'alter schema "{namespace}-automatic" '
            f'rename to "{namespace}-upstream"'
        )
        cn.execute(
            f'alter schema "{namespace}-automatic.timeserie" '
            f'rename to "{namespace}-upstream.timeserie"'
        )
        cn.execute(
            f'alter schema "{namespace}-automatic.snapshot" '
            f'rename to "{namespace}-upstream.snapshot"'
        )


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    e = create_engine(find_dburi(db_uri))

    tsh = timeseries(namespace)
    import pdb; pdb.set_trace()
