import click

from sqlalchemy import create_engine
from tshistory.util import find_dburi


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
