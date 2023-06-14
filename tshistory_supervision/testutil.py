from functools import partial

import responses

from tshistory.testutil import (
    read_request_bridge,
    with_http_bridge as basebridge
)


class with_http_bridge(basebridge):

    def __init__(self, uri, resp, wsgitester):
        super().__init__(uri, resp, wsgitester)

        resp.add_callback(
            responses.GET, uri + '/series/supervision',
            callback=partial(read_request_bridge, wsgitester)
        )
