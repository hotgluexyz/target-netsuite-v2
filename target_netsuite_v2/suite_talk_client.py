import json
import requests
from typing import List, Dict, Optional
from collections import defaultdict

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from target_hotglue.common import HGJSONEncoder

class SuiteTalkRestClient:
    ref_select_clauses = {
        "account": "account.id as internalId, account.acctName as name, account.acctNumber as number, account.externalId",
        "classification": "classification.id as internalId, classification.name, classification.externalId, subsidiary as subsidiaryId",
        "currency": "currency.id as internalId, currency.symbol, currency.name",
        "customer": "customer.id as internalId, customer.companyName as name, customer.externalId",
        "department": "department.id as internalId, department.name, department.externalId, subsidiary as subsidiaryId",
        "location": "location.id as internalId, location.name as name, location.externalId, location.subsidiary as subsidiaryId",
        "subsidiary": "subsidiary.id as internalId, subsidiary.name, subsidiary.externalId",
        "vendor": "vendor.id as internalId, vendor.companyName as name, vendor.externalId, vendor.subsidiary as subsidiaryId",
        "customercategory": "customercategory.id as internalid, customercategory.externalid as externalid, customercategory.name",
        "vendorcategory": "vendorcategory.id as internalId, vendorcategory.externalId as externalId, vendorcategory.name",
        "employee": "employee.id as internalid, employee.externalId as externalid, employee.firstname || ' ' || employee.lastname AS name, subsidiary as subsidiaryId",
        "item": "item.id as internalid, item.externalId as externalId, item.displayName as name"
    }

    ref_name_where_clauses = {
        "account": "account.acctName",
        "classification": "classification.name",
        "customer": "customer.companyName",
        "department": "department.name",
        "location": "location.name",
        "subsidiary": "subsidiary.name",
        "vendor": "vendor.companyName",
        "customercategory": "customercategory.name",
        "vendorcategory": "vendorcategory.name",
        "employee": "employee.firstname || ' ' || employee.lastname",
        "item": "item.displayName"
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

    def create_item(self, item):
        url = self.get_item_url(item)
        if not url:
            return None, False, "Unknown Item type and category"
        response = self._make_request(url, "POST", data=item)
        success, error_message = self._validate_response(response)
        record_id = self._extract_id_from_response_header(response.headers)
        return record_id, success, error_message

    def update_item(self, item_id, item):
        url = self.get_item_url(item)
        if not url:
            return None, False, "Unknown Item type and category"
        url += f"/{str(item_id)}"
        response = self._make_request(url, "PATCH", data=item)
        success, error_message = self._validate_response(response)
        record_id = self._extract_id_from_response_header(response.headers)
        return record_id, success, error_message

    def get_item_url(self, item: dict) -> str:
        item_type = item.get("type", "").lower()
        item_subtype = item.get("category", "").lower()
        if item_type == "invtpart":
            endpoint = "inventoryItem"
        elif item_type == "noninvtpart":
            if item_subtype == "sale":
                endpoint = "nonInventorySaleItem"
            elif item_subtype == "purchase":
                endpoint = "nonInventoryPurchaseItem"
            elif item_subtype == "resale":
                endpoint = "nonInventoryResaleItem"
            else:
                endpoint = None
        elif item_type == "service":
            if item_subtype == "sale":
                endpoint = "serviceSaleItem"
            elif item_subtype == "purchase":
                endpoint = "servicePurchaseItem"
            elif item_subtype == "resale":
                endpoint = "serviceResaleItem"
            else:
                endpoint = None
        elif item_type == "othercharge":
            if item_subtype == "sale":
                endpoint = "otherChargeSaleItem"
            elif item_subtype == "purchase":
                endpoint = "otherChargePurchaseItem"
            elif item_subtype == "resale":
                endpoint = "otherChargeResaleItem"
            else:
                endpoint = None
        else:
            endpoint = None

        if endpoint:
            return f"{self.record_url}/{endpoint}"

    def get_transaction_data(
        self,
        transaction_type,
        external_ids: Optional[List[str]] = None,
        record_ids: Optional[List[str]] = None,
        page_size=1000
    ) -> List[Dict]:

        if record_ids is not None and not record_ids and external_ids is not None and not external_ids:
            return True, None, []

        query = f"SELECT transaction.id as internalId, transaction.externalId as externalId, transaction.subsidiary as subsidiaryId FROM transaction WHERE transaction.type = '{transaction_type}'"
        where_clause = ""

        if record_ids:
            id_string = ",".join(str(id) for id in record_ids)
            where_clause = f"AND id IN ({id_string})"

        if external_ids:
            tran_id_string = ",".join(f"'{id}'" for id in external_ids)
            where_clause = f"AND externalId IN ({tran_id_string})"

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

            # SuiteQL response fields come in as lower case,
            # even when using `AS` syntax that includes capital letters
            for item in items:
                if "internalid" in item:
                    item["internalId"] = item.pop("internalid")
                if "externalid" in item:
                    item["externalId"] = item.pop("externalid")
                if "subsidiaryid" in item:
                    item["subsidiaryId"] = item.pop("subsidiaryid")

            all_items.extend(items)

            has_more = resp_json.get("hasMore", False)
            offset += limit

        return True, None, all_items

    def get_reference_data(
        self,
        record_type,
        record_ids: Optional[List[str]] = None,
        external_ids: Optional[List[str]] = None,
        names: Optional[List[str]] = None,
        page_size=1000
    ) -> List[Dict]:
        # Early exit if record_ids, external_ids, and names are provided but are all empty
        # This is done for cases where we pass an empty list or set after processing a batch looking for ids/external ids/names
        # Otherwise, we would simply not construct where clauses, and pull back everything.
        if record_ids is not None and external_ids is not None and names is not None and not record_ids and not external_ids and not names:
            return True, None, []

        select_clause = self.ref_select_clauses[record_type]
        where_clause = ""

        if record_ids:
            id_string = ",".join(str(id) for id in record_ids)
            where_clause = f"WHERE id IN ({id_string})"

        if external_ids:
            external_id_string = ",".join(f"'{id}'" for id in external_ids)

            if where_clause:
                where_clause = f"{where_clause} OR externalId IN ({external_id_string})"
            else:
                where_clause = f"WHERE externalId IN ({external_id_string})"

        if names and record_type in self.ref_name_where_clauses:
            names_string = ",".join(f"'{id}'" for id in names)

            if where_clause:
                where_clause = f"{where_clause} OR {self.ref_name_where_clauses[record_type]} IN ({names_string})"
            else:
                where_clause = f"WHERE {self.ref_name_where_clauses[record_type]} IN ({names_string})"

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

            # SuiteQL response fields come in as lower case,
            # even when using `AS` syntax that includes capital letters
            for item in items:
                if "internalid" in item:
                    item["internalId"] = item.pop("internalid")
                if "externalid" in item:
                    item["externalId"] = item.pop("externalid")
                if "subsidiaryid" in item:
                    item["subsidiaryId"] = item.pop("subsidiaryid")

            all_items.extend(items)

            has_more = resp_json.get("hasMore", False)
            offset += limit

        return True, None, all_items

    def get_invoice_items(self, external_ids=None):
        if external_ids is not None and not external_ids:
            return True, None, {}

        where_clause = ""

        if external_ids:
            external_id_string = ",".join(f"'{id}'" for id in external_ids)
            where_clause = f"AND t.externalid IN ({external_id_string})"

        query = "SELECT t.recordtype, tl.* FROM transaction t inner join transactionLine tl on tl.transaction = t.id WHERE mainline <> 'T'"
        if where_clause:
            query += f" {where_clause}"

        query_data = {"q": query}
        headers = {"Prefer": "transient"}

        response = self._make_request(
            url=self.suiteql_url,
            method="POST",
            data=query_data,
            params={},
            headers=headers
        )

        success, error_message = self._validate_response(response)
        if not success:
            return success, error_message, {}

        resp_json = response.json()
        items = resp_json.get("items", [])
        result = defaultdict(lambda: {"lineItems": []})

        for item in items:
            transaction_id = item["transaction"]
            if item.get("accountinglinetype"):
                result[transaction_id]["lineItems"].append(item)

        return True, None, dict(result)

    def get_bill_items(self, external_ids=None):
        if external_ids is not None and not external_ids:
            return True, None, {}

        where_clause = ""

        if external_ids:
            external_id_string = ",".join(f"'{id}'" for id in external_ids)
            where_clause = f"AND t.externalid IN ({external_id_string})"

        query = "SELECT t.recordtype, tl.* FROM transaction t inner join transactionLine tl on tl.transaction = t.id WHERE mainline <> 'T'"
        if where_clause:
            query += f" {where_clause}"

        query_data = {"q": query}
        headers = {"Prefer": "transient"}

        response = self._make_request(
            url=self.suiteql_url,
            method="POST",
            data=query_data,
            params={},
            headers=headers
        )

        success, error_message = self._validate_response(response)
        if not success:
            return success, error_message, {}

        resp_json = response.json()
        items = resp_json.get("items", [])
        result = defaultdict(lambda: {"lineItems": [], "expenses": []})

        for item in items:
            transaction_id = item["transaction"]
            category = "lineItems" if item.get("accountinglinetype") == "ASSET" else "expenses"
            result[transaction_id][category].append(item)

        return True, None, dict(result)

    def get_invoice_payments(self, invoice_ids=None):
        if invoice_ids is not None and not invoice_ids:
            return True, None, {}

        where_clause = ""

        if invoice_ids:
            external_id_string = ",".join(f"'{id}'" for id in invoice_ids)
            where_clause = f"and NTLL.PreviousDoc in ({external_id_string})"

        query = "SELECT DISTINCT NTLL.PreviousDoc transaction, NT.ID ID, NT.tranid, NT.transactionNumber, NT.account account, NT.TranDate, NT.Type, BUILTIN.DF(NT.Status) status, NT.ForeignTotal amount, currency, exchangeRate FROM NextTransactionLineLink AS NTLL INNER JOIN Transaction AS NT ON (NT.ID = NTLL.NextDoc) WHERE NT.recordtype = 'customerpayment'"
        if where_clause:
            query += f" {where_clause}"

        query_data = {"q": query}
        headers = {"Prefer": "transient"}

        response = self._make_request(
            url=self.suiteql_url,
            method="POST",
            data=query_data,
            params={},
            headers=headers
        )

        success, error_message = self._validate_response(response)
        if not success:
            return success, error_message, {}

        resp_json = response.json()
        payments = resp_json.get("items", [])
        result = defaultdict(lambda: {"payments": []})

        for payment in payments:
            transaction_id = payment["transaction"]
            result[transaction_id]["payments"].append(payment)

        return True, None, dict(result)

    def get_bill_payments(self, bill_ids=None):
        if bill_ids is not None and not bill_ids:
            return True, None, {}

        where_clause = ""

        if bill_ids:
            external_id_string = ",".join(f"'{id}'" for id in bill_ids)
            where_clause = f"and NTLL.PreviousDoc in ({external_id_string})"

        query = "SELECT DISTINCT NTLL.PreviousDoc transaction, NT.ID ID, NT.tranid, NT.transactionNumber, NT.account account, NT.TranDate, NT.Type, BUILTIN.DF(NT.Status) status, NT.ForeignTotal amount, currency, exchangeRate FROM NextTransactionLineLink AS NTLL INNER JOIN Transaction AS NT ON (NT.ID = NTLL.NextDoc) WHERE NT.recordtype = 'vendorpayment'"
        if where_clause:
            query += f" {where_clause}"

        query_data = {"q": query}
        headers = {"Prefer": "transient"}

        response = self._make_request(
            url=self.suiteql_url,
            method="POST",
            data=query_data,
            params={},
            headers=headers
        )

        success, error_message = self._validate_response(response)
        if not success:
            return success, error_message, {}

        resp_json = response.json()
        payments = resp_json.get("items", [])
        result = defaultdict(lambda: {"payments": []})

        for payment in payments:
            transaction_id = payment["transaction"]
            result[transaction_id]["payments"].append(payment)

        return True, None, dict(result)

    def get_default_addresses(self, entity_type: str, entity_ids: List[str]) -> Dict[int, Dict[str, Optional[Dict]]]:
        if not entity_ids:
            return True, None, {}

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
