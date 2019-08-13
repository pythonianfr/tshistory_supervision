import click

import tqdm
import pandas as pd
from sqlalchemy import create_engine
from tshistory.util import find_dburi
from collections import defaultdict

from tshistory_supervision.tsio import timeseries


@click.command(name='migrate-supervision-0.5-to-0.6')
@click.argument('dburi')
@click.option('--name')
@click.option('--namespace', default='tsh')
def migrate_supervision_dot_5_to_dot_6(dburi, name=None, namespace='tsh'):
    engine = create_engine(find_dburi(dburi))
    tsh = timeseries(namespace)
    if name:
        series = [name]
    else:
        series = tsh.list_series(engine)

    categories = defaultdict(list)

    bar = tqdm.tqdm(range(len(series)))
    for name in series:
        synth_idates = tsh.insertion_dates(engine, name)
        has_upstream = tsh.upstream.exists(engine, name)
        upstream_idates = (
            has_upstream and
            tsh.upstream.insertion_dates(engine, name) or
            []
        )
        if synth_idates and not upstream_idates:
            status = 'handcrafted'
        elif len(synth_idates) == len(upstream_idates):
            status = 'unsupervised'
        else:
            status = 'supervised'
        categories[status].append(name)

        meta = tsh.metadata(engine, name)
        meta['supervision_status'] = status
        with engine.begin() as cn:
            tsh.update_metadata(cn, name, meta, internal=True)

            # reclaim space
            if status in ('handcrafted', 'unsupervised'):
                if tsh.upstream.exists(cn, name):
                    tsh.upstream.delete(cn, name)

        bar.update()
    bar.close()

    print('unsupervised', len(categories['unsupervised']))
    print('handcrafted', len(categories['handcrafted']))
    print('supervised', len(categories['supervised']))


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    e = create_engine(find_dburi(db_uri))

    tsh = timeseries(namespace)
    import pdb; pdb.set_trace()
