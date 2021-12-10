import pandas as pd

import requests
from flask import make_response

from flask_restx import (
    Resource,
    reqparse
)
from tshistory import util

from tshistory.http.client import (
    Client,
    strft,
    unwraperror
)
from tshistory.http.server import httpapi
from tshistory.http.util import (
    enum,
    onerror,
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


class supervision_httpapi(httpapi):

    def routes(self):
        super().routes()

        tsa = self.tsa
        api = self.api
        nss = self.nss
        nsg = self.nsg

        @nss.route('/supervision')
        class series_supervision(Resource):

            @api.expect(edited)
            @onerror
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
                )
                metadata = tsa.metadata(args.name, all=True)
                assert metadata is not None, f'series {args.name} has no metadata'

                if args.format == 'json':
                    if series is not None:
                        df = pd.DataFrame()
                        df['series'] = series
                        df['markers'] = markers
                        response = make_response(
                            df.to_json(orient='index',
                                       date_format='iso')
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


class SupervisionClient(Client):

    def __repr__(self):
        return f"tshistory-supervision-http-client(uri='{self.uri}')"

    @unwraperror
    def edited(self, name,
               revision_date=None,
               from_value_date=None,
               to_value_date=None):
        args = {
            'name': name,
            'format': 'tshpack',
        }
        if revision_date:
            args['insertion_date'] = strft(revision_date)
        if from_value_date:
            args['from_value_date'] = strft(from_value_date)
        if to_value_date:
            args['to_value_date'] = strft(to_value_date)
        res = requests.get(
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
        meta = self.metadata(name, all=True)
        return meta.get('supervision_status', 'unknown')
