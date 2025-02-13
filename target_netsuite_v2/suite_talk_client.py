import json
import requests

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from target_hotglue.common import HGJSONEncoder

class SuiteTalkRestClient:
    def __init__(self, config):
        self.config = config

    @property
    def url_account(self) -> str:
        return self.config["ns_account"].replace("_", "-").replace("SB", "sb")

    @property
    def url_prefix(self) -> str:
        return f"https://{self.url_account}.suitetalk.api.netsuite.com/services/rest"

    @property
    def record_url(self) -> str:
        return f"{self.url_prefix}/record/v1"

    @property
    def suiteql_url(self) -> str:
        return f"{self.url_prefix}/query/v1/suiteql"

    def _make_request(self, url, http_method, record):
        headers = {"Content-Type": "application/json"}
        data = (
            json.dumps(record, cls=HGJSONEncoder)
            if record
            else None
        )
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        return requests.request(
            method=http_method,
            url=url,
            params={},
            headers=headers,
            data=data,
            verify=True,
            auth=oauth
        )

    def update_record(self, endpoint, record_id, record):
        url = f"{self.record_url}{endpoint}/{record_id}"
        return self._make_request(url, "PATCH", record)

    def create_record(self, endpoint, record):
        url = f"{self.record_url}{endpoint}"
        return self._make_request(url, "POST", record)
