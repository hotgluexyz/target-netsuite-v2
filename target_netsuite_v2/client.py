import json
import requests

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from singer_sdk.exceptions import FatalAPIError
from target_hotglue.client import HotglueBaseSink, HotglueBatchSink, HotglueSink
from target_hotglue.common import HGJSONEncoder
from typing import Dict, List

class NetSuiteBaseSink(HotglueBaseSink):
    @property
    def url_account(self) -> str:
        return self.config["ns_account"].replace("_", "-").replace("SB", "sb")

    @property
    def base_url(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return (
            f"https://{self.url_account}.suitetalk.api.netsuite.com/services/rest/record/v1"
        )

    def _extract_id_from_response_header(self, headers):
        return headers["Location"].split("/")[-1]

    def request_api(self, http_method, endpoint=None, params={}, request_data=None, headers={}, verify=True):
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data
            else None
        )

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            verify=verify,
            auth=oauth
        )
        self.validate_response(response)

        return response

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code >= 400:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        return json.dumps(response.json().get("o:errorDetails"))

    def record_exists(self, record: dict) -> bool:
        False

    def upsert_record(self, record: dict, context: dict):
        if self.record_exists(record, context):
            response = self.request_api("PATCH", request_data=record, endpoint=f"{self.endpoint}/{record['internalId']}")
        else:
            response = self.request_api("POST", request_data=record)

        id = self._extract_id_from_response_header(response.headers)
        return id, response.ok, dict()


class NetSuiteSink(NetSuiteBaseSink, HotglueSink):
    pass
