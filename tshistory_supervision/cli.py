import click

import tqdm
import pandas as pd
from sqlalchemy import create_engine
from tshistory.util import find_dburi
from collections import defaultdict

from tshistory_supervision.tsio import timeseries


def compute_supervision_status(tsh, engine, name):
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
    return status


@click.command(name='fix-supervision-status')
@click.argument('dburi')
@click.option('--name')
@click.option('--namespace', default='tsh')
def fix_supervision_status(dburi, name=None, namespace='tsh'):
    engine = create_engine(find_dburi(dburi))
    tsh = timeseries(namespace)
    if name:
        series = [name]
    else:
        series = [
            name for name, stype in tsh.list_series(engine).items()
            if stype == 'primary'
        ]

    categories = defaultdict(list)

    bar = tqdm.tqdm(range(len(series)))
    for name in series:
        meta = tsh.metadata(engine, name)
        if 'supervision_status' in meta:
            continue

        status = compute_supervision_status(tsh, engine, name)
        categories[status].append(name)
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


@click.command(name='list-supervised-series-mismatch')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def list_mismatch(db_uri, namespace='tsh'):
    e = create_engine(find_dburi(db_uri))

    tsh = timeseries(namespace)
    series = set(tsh.list_series(e))
    upstream = set(tsh.upstream.list_series(e))
    diff = upstream - series
    if not diff:
        print('no mismatch')
        return

    print(f'found {len(diff)} series in upstream')
    for name in sorted(diff):
        assert (False, True) == (tsh.exists(e, name), tsh.upstream.exists(e, name))
        print(name)


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    e = create_engine(find_dburi(db_uri))

    tsh = timeseries(namespace)
    import pdb; pdb.set_trace()
