from tshistory.migrate import (
    fix_user_metadata,
    migrate_metadata,
    Migrator as _Migrator
)
from tshistory_supervision import __version__


class Migrator(_Migrator):
    _order = 1
    _known_version = __version__
    _package = 'tshistory-supervision'

    def initial_migration(self):
        print('initial migration')
        migrate_metadata(self.engine, f'{self.namespace}-upstream', self.interactive)
        fix_user_metadata(self.engine, f'{self.namespace}-upstream', self.interactive)
