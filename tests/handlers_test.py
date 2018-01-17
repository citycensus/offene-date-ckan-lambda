import unittest
import httpretty
from moto import mock_s3

import handlers

class TestHandlers(object):

    @httpretty.activate
    def test_get_all_cities(self):
        httpretty.register_uri(httpretty.POST, "https://offenedaten.de/api/action/organization_list",
            body='{"success": true, "result": []}',
            content_type="application/json")
        #assert handlers.get_all_cities({},{}) == {}

