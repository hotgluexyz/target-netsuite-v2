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

        return response

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        return json.dumps(response.json().get("o:errorDetails"))

    def record_exists(self, record: dict) -> bool:
        False

class NetSuiteSink(NetSuiteBaseSink, HotglueSink):
    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code >= 400:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def upsert_record(self, record: dict, context: dict):
        if self.record_exists(record, context):
            response = self.request_api("PATCH", request_data=record, endpoint=f"{self.endpoint}/{record['internalId']}")
        else:
            response = self.request_api("POST", request_data=record)
        self.validate_response(response)
        id = self._extract_id_from_response_header(response.headers)
        return id, response.ok, dict()

class NetSuiteBatchSink(NetSuiteBaseSink, HotglueBatchSink):
    max_size = 1

    # process_batch_record is a HotglueBatchSync expected override
    # --> this is where we do the mapping

    # handle_batch_response is a HotglueBatchSync expected override
    # --> this is where we transform results into a state_updates dictionary

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        results = self.make_batch_request(records)

        state_updates = self.handle_batch_response(results)

        for state in state_updates.get("state_updates", list()):
            self.update_state(state)

    # HotglueBatchSync expected override
    def make_batch_request(self, records: List[Dict]):
        results = []
        for record in records:
            id, success, state = self.upsert_record(record, {})
            results.append({"id": id, "success": success, "state": state})
        return results

    def upsert_record(self, record: dict, context: dict):
        if self.record_exists(record, context):
            response = self.request_api("PATCH", request_data=record, endpoint=f"{self.endpoint}/{record['internalId']}")
        else:
            response = self.request_api("POST", request_data=record)
        success, error_message = self.validate_response(response)
        id = self._extract_id_from_response_header(response.headers)
        return id, success, dict()

    def validate_response(self, response: requests.Response) -> tuple[bool, str | None]:
        """Validate HTTP response.

        Returns:
            tuple[bool, str | None]: Returns (True, None) if successful, (False, error_message) if validation fails
        """
        if response.status_code >= 400:
            msg = self.response_error_message(response)
            return False, msg
        else:
            return True, None
