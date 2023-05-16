from dbcache import api as dbapi

from tshistory.util import read_versions

from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata
)


def run_migrations(engine, namespace, interactive=False):
    print('Running migrations for tshistory_supervision.')
    # determine versions
    storens = f'{namespace}-kvstore'
    stored_version, known_version = read_versions(
        str(engine.url),
        namespace,
        'tshistory-supervision-version'
    )

    if stored_version is None:
        # first time
        from tshistory_supervision import __version__ as known_version
        store = dbapi.kvstore(str(engine.url), namespace=storens)
        # we probably want to go further
        initial_migration(engine, namespace, interactive)
        store.set('tshistory-supervision-version', known_version)


def initial_migration(engine, namespace, interactive):
    print('initial migration')
    migrate_metadata(engine, f'{namespace}-upstream', interactive)
    fix_user_metadata(engine, f'{namespace}-upstream', interactive)
