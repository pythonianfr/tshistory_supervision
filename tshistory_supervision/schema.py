from tshistory.schema import tsschema


class supervision_schema(tsschema):

    def create(self, engine, **kw):
        super().create(engine, **kw)
        # complete the base tshistory schema, by delegation
        tsschema(f'{self.namespace}-upstream').create(engine)
