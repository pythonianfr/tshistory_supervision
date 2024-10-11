import simplejson as json
import pandas as pd

from flask import make_response

from flask_restx import (
    inputs,
    Resource,
    reqparse
)
from tshistory import util

from tshistory.http.client import (
    httpclient,
    strft,
    unwraperror
)
from tshistory.http.server import httpapi
from tshistory.http.util import (
    enum,
    onerror,
    required_roles,
    utcdt
)


base = reqparse.RequestParser()
base.add_argument(
    'name',
    type=str,
    required=True,
    help='timeseries name'
)

edited = base.copy()
edited.add_argument(
    'insertion_date', type=utcdt, default=None,
    help='select a specific version'
)
edited.add_argument(
    'from_value_date', type=utcdt, default=None
)
edited.add_argument(
    'to_value_date', type=utcdt, default=None
)
edited.add_argument(
    'format', type=enum('json', 'tshpack'), default='json'
)
edited.add_argument(
    'horizon', type=str, default=None,
    help='override from/to_value_date'
)
edited.add_argument(
    'inferred_freq', type=inputs.boolean, default=False,
    help='re-index series on a inferred frequency'
)
edited.add_argument(
    'tzone', type=str, default='UTC',
    help='Convert tz-aware series into this time zone before sending'
)
edited.add_argument(
    '_keep_nans', type=inputs.boolean, default=False,
    help='keep erasure information'
)


class supervision_httpapi(httpapi):

    def routes(self):
        super().routes()

        tsa = self.tsa
        api = self.api
        nss = self.nss

        @nss.route('/supervision')
        class series_supervision(Resource):

            @api.expect(edited)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = edited.parse_args()
                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')
                if getattr(tsa, 'formula', False):
                    if tsa.formula(args.name):
                        api.abort(404, f'`{args.name}` is a formula')

                series, markers = tsa.edited(
                    args.name,
                    revision_date=args.insertion_date,
                    from_value_date=args.from_value_date,
                    to_value_date=args.to_value_date,
                    inferred_freq=args.get('inferred_freq'),
                    _keep_nans=args._keep_nans
                )
                metadata = tsa.internal_metadata(args.name)
                if metadata['tzaware'] and args.tzone.upper() != 'UTC':
                    series.index = series.index.tz_convert(args.tzone)
                    markers.index = markers.index.tz_convert(args.tzone)

                if args.format == 'json':
                    if series is not None:
                        df = pd.DataFrame()
                        df['series'] = series
                        df['markers'] = markers
                        out = {
                            k.isoformat(): v
                            for k, v in df.to_dict(orient='index').items()
                        }
                        response = make_response(
                            json.dumps(out, ignore_nan=True)
                        )
                    else:
                        response = make_response('null')
                    response.headers['Content-Type'] = 'text/json'
                    response.status_code = 200
                    return response

                assert args.format == 'tshpack'
                markersmeta = util.series_metadata(markers)
                response = make_response(
                    util.pack_many_series(
                        [
                            (metadata, series),
                            (markersmeta, markers)
                        ]
                    )
                )
                response.headers['Content-Type'] = 'application/octet-stream'
                response.status_code = 200
                return response


class supervision_httpclient(httpclient):
    index = 0.5

    def __repr__(self):
        return f"tshistory-supervision-http-client(uri='{self.uri}')"

    @unwraperror
    def edited(self, name,
               revision_date=None,
               from_value_date=None,
               to_value_date=None,
               inferred_freq=False,
               _keep_nans=False):
        args = {
            'name': name,
            '_keep_nans': json.dumps(_keep_nans),
            'format': 'tshpack',
        }
        if revision_date:
            args['insertion_date'] = strft(revision_date)
        if from_value_date:
            args['from_value_date'] = strft(from_value_date)
        if to_value_date:
            args['to_value_date'] = strft(to_value_date)
        if inferred_freq:
            args['inferred_freq'] = inferred_freq
        res = self.session.get(
            f'{self.uri}/series/supervision', params=args
        )
        if res.status_code == 404:
            return None
        if res.status_code == 200:
            series, markers = util.unpack_many_series(res.content)
            return series, markers

        return res

    @unwraperror
    def supervision_status(self, name):
        meta = self.internal_metadata(name)
        return meta.get('supervision_status', 'unknown')
