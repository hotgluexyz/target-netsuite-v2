import json
import requests
import hashlib

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBaseSink, HotglueSink
from target_hotglue.common import HGJSONEncoder

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
        location = headers.get("Location")
        if not location:
            return None
        return location.split("/")[-1]

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

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("internalId"))

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        return json.dumps(response.json().get("o:errorDetails"))

    def build_record_hash(self, record: dict):
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def get_existing_state(self, hash: str):
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        if existing_state:
            self.latest_state["summary"][self.name]["existing"] += 1

        return existing_state

class NetSuiteSink(NetSuiteBaseSink, HotglueSink):
    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_codeu >= 400:
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

class NetSuiteBatchSink(NetSuiteBaseSink, BatchSink):
    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        for record in raw_records:
            self.process_batch_record(record)

    def process_batch_record(self, record):
        preprocessed = self.preprocess_batch_record(record)
        hash = self.build_record_hash(preprocessed)
        existing_state = self.get_existing_state(hash)
        external_id = preprocessed.get("externalId")

        if existing_state:
            self.update_state(existing_state, is_duplicate=True)
            return

        id, success, state = self.upsert_record(preprocessed, {})

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if external_id:
            state["externalId"] = external_id

        self.update_state(state)

    def upsert_record(self, record: dict, context: dict):
        id = None
        state = {}

        if self.record_exists(record, context):
            id = record['internalId']
            response = self.request_api("PATCH", request_data=record, endpoint=f"{self.endpoint}/{id}")
        else:
            response = self.request_api("POST", request_data=record)
            id = self._extract_id_from_response_header(response.headers)

        success, error_message = self.validate_response(response)

        if error_message:
            state["error"] = error_message

        return id, success, state

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
