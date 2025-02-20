import json
import requests
from typing import List, Dict, Optional

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from target_hotglue.common import HGJSONEncoder

class SuiteTalkRestClient:
    ref_select_clauses = {
        "account": "account.id as internalId, account.acctName as name, account.externalId",
        "classification": "classification.id as internalId, classification.name, classification.externalId",
        "currency": "currency.id as internalId, currency.symbol, currency.name",
        "customer": "customer.id as internalId, customer.companyName as name, customer.externalId",
        "department": "department.id as internalId, department.name, department.externalId",
        "location": "location.id as internalId, location.name as name, location.externalId",
        "subsidiary": "subsidiary.id as internalId, subsidiary.name, subsidiary.externalId",
        "vendor": "vendor.id as internalId, vendor.companyName as name, vendor.externalId"
    }

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

    def get_reference_data(
        self,
        record_type,
        record_ids: Optional[List[str]] = None,
        external_ids: Optional[List[str]] = None,
        page_size=1000
    ) -> List[Dict]:
        select_clause = self.ref_select_clauses[record_type]
        where_clause = ""
        if record_ids:
            id_string = ",".join(str(id) for id in record_ids)
            where_clause = f"WHERE id IN ({id_string})"

        if external_ids:
            external_id_string = ",".join(f"'{id}'" for id in external_ids)

            if where_clause:
                id_string = ",".join(str(id) for id in record_ids)
                where_clause = f"WHERE (id IN ({id_string}) OR externalId IN ({external_id_string}))"
            else:
                where_clause = f"WHERE external_id IN ({external_id_string})"

        query = f"SELECT {select_clause} FROM {record_type}"
        if where_clause:
            query += f" {where_clause}"

        all_items = []
        offset = 0
        limit = min(page_size, 1000)
        has_more = True

        while has_more:
            query_data = {"q": query}
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

            # SuiteQL response fields come in as lower case
            for item in items:
                if "internalid" in item:
                    item["internalId"] = item.pop("internalid")
                if "externalid" in item:
                    item["externalId"] = item.pop("externalid")

            all_items.extend(items)

            has_more = resp_json.get("hasMore", False)
            offset += limit

        return True, None, all_items

    def get_default_addresses(self, entity_type: str, entity_ids: List[str]) -> Dict[int, Dict[str, Optional[Dict]]]:
        entity_ids_str = ",".join(map(str, entity_ids))
        entity_id_field = f"{entity_type}.id"
        addressbook_table = f"{entity_type}addressbook"
        addressbook_entity_address_table = f"{entity_type}addressbookentityaddress"

        query = (
            f"SELECT {entity_id_field} as entityid, {addressbook_entity_address_table}.*, "
            f"{addressbook_table}.defaultshipping, {addressbook_table}.defaultbilling "
            f"FROM {entity_type} "
            f"JOIN {addressbook_table} ON ({entity_id_field} = {addressbook_table}.entity) "
            f"JOIN {addressbook_entity_address_table} ON ({addressbook_table}.addressbookaddress = {addressbook_entity_address_table}.nkey) "
            f"WHERE {entity_id_field} IN ({entity_ids_str}) AND "
            f"({addressbook_table}.defaultbilling = 'T' OR {addressbook_table}.defaultshipping = 'T')"
        )

        query_data = {"q": query}
        headers = {"Prefer": "transient"}

        response = self._make_request(
            url=self.suiteql_url,
            method="POST",
            data=query_data,
            headers=headers
        )

        success, error_message = self._validate_response(response)
        if not success:
            return success, error_message, []

        resp_json = response.json()
        items = resp_json.get("items", [])

        default_addresses = {entity_id: {"billing": None, "shipping": None} for entity_id in entity_ids}

        for item in items:
            entity_id = item.get("entityid")
            if entity_id:
                if item.get("defaultbilling") == 'T':
                    default_addresses[entity_id]["billing"] = item
                if item.get("defaultshipping") == 'T':
                    default_addresses[entity_id]["shipping"] = item

        return True, None, default_addresses

    def get_customer_default_addresses(self, customer_ids: List[str]) -> Dict[int, Dict[str, Optional[Dict]]]:
        return self.get_default_addresses("customer", customer_ids)

    def get_vendor_default_addresses(self, vendor_ids: List[str]) -> Dict[int, Dict[str, Optional[Dict]]]:
        return self.get_default_addresses("vendor", vendor_ids)

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
