from tshistory.schema import init as tshinit, reset as tshreset


def init(engine, meta, namespace='tsh'):
    tshinit(engine, meta, namespace)
    tshinit(engine, meta, '{}-automatic'.format(namespace))


def reset(engine, namespace='tsh'):
    tshreset(engine, namespace)
    tshreset(engine, '{}-automatic'.format(namespace))
