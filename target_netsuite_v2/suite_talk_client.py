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

    def update_record(self, record_type, record_id, record):
        url = f"{self.record_url}/{record_type}/{record_id}"
        response = self._make_request(url, "PATCH", data=record)
        success, error_message = self._validate_response(response)
        return record_id, success, error_message

    def create_record(self, record_type, record):
        url = f"{self.record_url}/{record_type}"
        response = self._make_request(url, "POST", data=record)
        success, error_message = self._validate_response(response)
        record_id = self._extract_id_from_response_header(response.headers)
        return record_id, success, error_message

    def get_records(self, record_type, record_ids=None, page_size=1000):
        suiteql_query_string = f"SELECT * FROM {record_type}"
        if record_ids:
            id_string = ",".join(str(id) for id in record_ids)
            suiteql_query_string += f" WHERE ID IN ({id_string})"

        all_items = []
        offset = 0
        limit = min(page_size, 1000)
        has_more = True

        while has_more:
            query_data = {"q": suiteql_query_string}
            params = {"offset": offset, "limit": limit}
            headers = {"Prefer": "transient"}

            response = self._make_request(
                url=self.suiteql_url,
                method="POST",
                data=query_data,
                params=params,
                headers=headers
            )

            success, error_message = self._validate_response(response)
            if not success:
                return success, error_message, []

            resp_json = response.json()
            items = resp_json.get("items", [])
            all_items.extend(items)

            has_more = resp_json.get("hasMore", False)
            offset += limit

        return True, None, all_items

    def _make_request(self, url, method, data=None, params=None, headers=None):
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)

        request_params = params or {}

        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        json_data = json.dumps(data, cls=HGJSONEncoder) if data else None

        return requests.request(
            method=method,
            url=url,
            params=request_params,
            headers=request_headers,
            data=json_data,
            verify=True,
            auth=oauth
        )

    def _validate_response(self, response: requests.Response) -> tuple[bool, str | None]:
        if response.status_code >= 400:
            msg = self._response_error_message(response)
            return False, msg
        else:
            return True, None

    def _response_error_message(self, response: requests.Response) -> str:
        return json.dumps(response.json().get("o:errorDetails"))

    def _extract_id_from_response_header(self, headers):
        location = headers.get("Location")
        if not location:
            return None
        return location.split("/")[-1]
