from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata,
    Migrator as _Migrator,
    version
)
from tshistory_supervision import __version__


class Migrator(_Migrator):
    _order = 1
    _package_version = __version__
    _package = 'tshistory-supervision'

    def initial_migration(self):
        print('initial migration')
        migrate_metadata(self.engine, f'{self.namespace}-upstream', self.interactive)
        fix_user_metadata(self.engine, f'{self.namespace}-upstream', self.interactive)


@version('tshistory-supervision', '0.13.0')
def migrate_revision_table(engine, namespace, interactive):
    from tshistory.migrate import migrate_add_diffstart_diffend

    migrate_add_diffstart_diffend(engine, f'{namespace}-upstream', interactive)
