import json
import requests
from typing import List, Dict, Optional, Set
from collections import defaultdict

from oauthlib import oauth1
from requests_oauthlib import OAuth1
from target_hotglue.common import HGJSONEncoder

class SuiteTalkRestClient:
    ITEM_ENDPOINTS = {
        "invtpart": "inventoryItem",
        "noninvtpart": {
            "sale": "nonInventorySaleItem",
            "purchase": "nonInventoryPurchaseItem",
            "resale": "nonInventoryResaleItem",
        },
        "service": {
            "sale": "serviceSaleItem",
            "purchase": "servicePurchaseItem",
            "resale": "serviceResaleItem",
        },
        "othercharge": {
            "sale": "otherChargeSaleItem",
            "purchase": "otherChargePurchaseItem",
            "resale": "otherChargeResaleItem",
        },
    }

    ref_select_clauses = {
        "account": "account.id as internalId, account.acctName as name, account.acctNumber as number, account.externalId",
        "classification": "classification.id as internalId, classification.name, classification.externalId, subsidiary as subsidiaryId",
        "currency": "currency.id as internalId, currency.symbol, currency.name",
        "customer": "customer.id as internalId, customer.companyName as name, customer.externalId, customer.entityid as entityId",
        "department": "department.id as internalId, department.name, department.externalId, subsidiary as subsidiaryId",
        "location": "location.id as internalId, location.name as name, location.externalId, location.subsidiary as subsidiaryId",
        "subsidiary": "subsidiary.id as internalId, subsidiary.name, subsidiary.externalId",
        "vendor": "vendor.id as internalId, vendor.companyName as name, vendor.externalId, vendor.subsidiary as subsidiaryId, vendor.entityid as entityId",
        "customercategory": "customercategory.id as internalid, customercategory.externalid as externalid, customercategory.name",
        "vendorcategory": "vendorcategory.id as internalId, vendorcategory.externalId as externalId, vendorcategory.name",
        "employee": "employee.id as internalid, employee.externalId as externalid, employee.firstname || ' ' || employee.lastname AS name, subsidiary as subsidiaryId",
        "item": "item.id as internalid, item.externalId as externalId, item.fullName as name, item.itemid as itemId",
        "salestaxitem": "salestaxitem.id as internalid, salestaxitem.name as name, salestaxitem.taxtype, customrecord_ste_taxrate.custrecord_ste_taxrate_rate as taxrate"
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
        "item": "item.fullName"
    }

    ref_join_clauses = {
        "salestaxitem": "inner join customrecord_ste_taxrate on customrecord_ste_taxrate.custrecord_ste_taxrate_taxcode = salestaxitem.id"
    }

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

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

    def _resolve_item_endpoint(self, item_type: str, item_subtype: str) -> Optional[str]:
        endpoint = self.ITEM_ENDPOINTS.get(item_type)
        if isinstance(endpoint, dict):
            return endpoint.get(item_subtype)

        return endpoint

    def _normalize_suiteql_item_fields(self, item, field_map):
        for source_field, target_field in field_map.items():
            if source_field in item:
                item[target_field] = item.pop(source_field)

    def _run_suiteql_query(self, query, page_size, field_map):
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

            for item in items:
                self._normalize_suiteql_item_fields(item, field_map)

            all_items.extend(items)

            has_more = resp_json.get("hasMore", False)
            offset += limit

        return True, None, all_items

    def _build_reference_filters(
        self,
        record_type,
        record_ids,
        external_ids,
        names,
        entity_ids,
        item_ids
    ):
        filters = []

        if record_ids:
            id_string = ",".join(str(record_id) for record_id in record_ids)
            filters.append(f"id IN ({id_string})")

        if external_ids:
            external_id_string = ",".join(f"'{external_id}'" for external_id in external_ids)
            filters.append(f"externalId IN ({external_id_string})")

        if names and record_type in self.ref_name_where_clauses:
            names_string = ",".join(f"'{name}'" for name in names)
            name_field = self.ref_name_where_clauses[record_type]
            filters.append(f"{name_field} IN ({names_string})")

        if entity_ids:
            entity_id_string = ",".join(f"'{entity_id}'" for entity_id in entity_ids)
            filters.append(f"entityId IN ({entity_id_string})")

        if item_ids:
            item_ids_str = ",".join(f"'{item_id}'" for item_id in item_ids)
            filters.append(f"itemId IN ({item_ids_str})")

        return filters

    def get_item_url(self, item: dict) -> str:
        item_type = item.get("type", "").lower()
        item_subtype = item.get("category", "").lower()
        endpoint = self._resolve_item_endpoint(item_type, item_subtype)

        if endpoint:
            return f"{self.record_url}/{endpoint}"

    def safe_int_convert(self, s: str, default_value: int) -> int:
        try:
            return int(s)
        except ValueError:
            return default_value

    def get_transaction_data(
        self,
        transaction_type,
        external_ids: Optional[List[str]] = None,
        record_ids: Optional[List[str]] = None,
        tran_ids: Optional[List[str]] = None,
        page_size=1000,
        extra_select_statement: Optional[str] = ''
    ) -> List[Dict]:

        if record_ids is not None and not record_ids and external_ids is not None and not external_ids and not tran_ids:
            return True, None, []
        
        if extra_select_statement:
            extra_select_statement = f", {extra_select_statement}"

        query = f"SELECT transaction.id as internalId, transaction.tranid as tranId, transaction.externalId as externalId, transaction.subsidiary as subsidiaryId{extra_select_statement} FROM transaction WHERE transaction.type = '{transaction_type}'"
        where_clauses = []

        if record_ids:
            # id has to be integer or the query will fail
            # so we convert to integer if possible, otherwise we use 0
            # this is a workaround for it not to break the query for
            # other ids or other filters
            id_string = ",".join(str(self.safe_int_convert(id, 0)) for id in record_ids)
            where_clauses.append(f"id IN ({id_string})")

        if tran_ids:
            tran_id_string = ",".join(f"'{id}'" for id in tran_ids)
            where_clauses.append(f"tranId IN ({tran_id_string})")

        if external_ids:
            external_ids_str = ",".join(f"'{id}'" for id in external_ids)
            where_clauses.append(f"externalId IN ({external_ids_str})")

        if where_clauses:
            where_statement = " OR ".join(where_clauses)
            query += f" AND ({where_statement})"

        return self._run_suiteql_query(
            query,
            page_size,
            {
                "internalid": "internalId",
                "externalid": "externalId",
                "subsidiaryid": "subsidiaryId",
                "tranid": "tranId",
            }
        )

    def get_reference_data(
        self,
        record_type,
        record_ids: Optional[List[str]] = None,
        external_ids: Optional[List[str]] = None,
        names: Optional[List[str]] = None,
        entity_ids: Optional[List[str]] = None,
        item_ids: Optional[List[str]] = None,
        page_size=1000,
        allow_empty_filters=False
    ) -> List[Dict]:
        # Early exit if record_ids, external_ids, and names are provided but are all empty
        # This is done for cases where we pass an empty list or set after processing a batch looking for ids/external ids/names
        # Otherwise, we would simply not construct where clauses, and pull back everything.
        if (
            not record_ids
            and not external_ids
            and not names
            and not entity_ids
            and not item_ids
            and not allow_empty_filters
        ):
            return True, None, []

        select_clause = self.ref_select_clauses[record_type]
        where_filters = self._build_reference_filters(
            record_type,
            record_ids,
            external_ids,
            names,
            entity_ids,
            item_ids
        )
        where_clause = f" WHERE {' OR '.join(where_filters)}" if where_filters else ""

        query = f"SELECT {select_clause} FROM {record_type}"

        if record_type in self.ref_join_clauses:
            query += f" {self.ref_join_clauses[record_type]}"

        query += where_clause

        return self._run_suiteql_query(
            query,
            page_size,
            {
                "internalid": "internalId",
                "externalid": "externalId",
                "subsidiaryid": "subsidiaryId",
                "entityid": "entityId",
                "itemid": "itemId",
                "taxtype": "taxType",
                "taxrate": "taxRate",
            }
        )

    def get_purchase_order_items(self, purchase_order_ids):
        if not purchase_order_ids:
            return True, None, {}

        purchase_order_ids_string = ",".join(f"'{id}'" for id in purchase_order_ids)

        query = f"SELECT t.recordtype, tl.* FROM transaction t inner join transactionLine tl on tl.transaction = t.id WHERE mainline <> 'T' AND t.type = 'PurchOrd' AND t.id IN ({purchase_order_ids_string})"

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
            result[transaction_id]["lineItems"].append(item)

        return True, None, dict(result)

    def get_invoice_items(self, invoice_ids: List[str]):
        if invoice_ids is not None and not invoice_ids:
            return True, None, {}

        where_clause = ""

        if invoice_ids:
            invoice_id_string = ",".join(f"'{id}'" for id in invoice_ids)
            where_clause = f"AND t.id IN ({invoice_id_string})"

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

    def get_bill_items(self, bill_ids: List[str]):
        if bill_ids is not None and not bill_ids:
            return True, None, {}

        where_clause = ""

        if bill_ids:
            bill_id_string = ",".join(f"'{id}'" for id in bill_ids)
            where_clause = f"AND t.id IN ({bill_id_string})"

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

    def get_vendor_credit_items(self, vendor_credit_ids):
        if not vendor_credit_ids:
            return True, None, {}

        vendor_credit_ids_string = ",".join(f"'{id}'" for id in vendor_credit_ids)

        query = f"SELECT t.recordtype, tl.* FROM transaction t inner join transactionLine tl on tl.transaction = t.id WHERE mainline <> 'T' AND t.recordtype = 'vendorcredit' AND t.id IN ({vendor_credit_ids_string})"

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

    def get_invoice_payments(self, invoice_ids: Optional[Set]=None, ids: Optional[Set]=None, external_ids: Optional[Set]=None, tran_ids: Optional[Set]=None, aggregate_payments: Optional[bool]=True):
        if invoice_ids is not None and not invoice_ids and not tran_ids:
            return True, None, {}

        where_clauses = []

        if invoice_ids:
            external_id_string = ",".join(f"'{id}'" for id in invoice_ids)
            where_clauses.append(f"NTLL.PreviousDoc in ({external_id_string})")

        if tran_ids:
            tran_id_string = ",".join(f"'{id}'" for id in tran_ids)
            where_clauses.append(f"NT.tranid in ({tran_id_string})")

        if ids:
            ids_string = ",".join(f"{id}" for id in ids)
            where_clauses.append(f"NT.ID in ({ids_string})")

        if external_ids:
            external_ids_string = ",".join(f"'{id}'" for id in external_ids)
            where_clauses.append(f"NT.externalId in ({external_ids_string})")

        query = "SELECT DISTINCT NTLL.PreviousDoc transaction, NT.ID ID, NT.ID internalId, NT.externalId, NT.tranid, NT.transactionNumber, NT.account account, NT.TranDate, NT.Type, BUILTIN.DF(NT.Status) status, NT.ForeignTotal amount, currency, exchangeRate FROM NextTransactionLineLink AS NTLL INNER JOIN Transaction AS NT ON (NT.ID = NTLL.NextDoc) WHERE NT.recordtype = 'customerpayment'"
        if where_clauses:
            where_statement = " OR ".join(where_clauses)
            query += f" AND ({where_statement})"

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

        if not aggregate_payments:
            for payment in payments:
                if "internalid" in payment:
                    payment["internalId"] = payment.pop("internalid")
                if "externalid" in payment:
                    payment["externalId"] = payment.pop("externalid")
                if "tranid" in payment:
                    payment["tranId"] = payment.pop("tranid")

            return True, None, payments

        result = defaultdict(lambda: {"payments": []})

        for payment in payments:
            transaction_id = payment["transaction"]
            result[transaction_id]["payments"].append(payment)

        return True, None, dict(result)

    def get_bill_payments(self, bill_ids: Optional[Set]=None, ids: Optional[Set]=None, external_ids: Optional[Set]=None, tran_ids: Optional[Set]=None, aggregate_payments: Optional[bool]=True):
        if bill_ids is not None and not bill_ids and not tran_ids:
            return True, None, {}

        where_clauses = []

        if bill_ids:
            external_id_string = ",".join(f"'{id}'" for id in bill_ids)
            where_clauses.append(f"NTLL.PreviousDoc in ({external_id_string})")

        if ids:
            ids_string = ",".join(f"{id}" for id in ids)
            where_clauses.append(f"NT.ID in ({ids_string})")

        if tran_ids:
            tran_id_string = ",".join(f"'{id}'" for id in tran_ids)
            where_clauses.append(f"NT.tranid in ({tran_id_string})")

        if external_ids:
            external_ids_string = ",".join(f"'{id}'" for id in external_ids)
            where_clauses.append(f"NT.externalId in ({external_ids_string})")

        query = "SELECT DISTINCT NTLL.PreviousDoc transaction, NT.ID ID, NT.ID internalId, NT.tranid, NT.externalId, NT.transactionNumber, NT.account account, NT.TranDate, NT.Type, BUILTIN.DF(NT.Status) status, NT.ForeignTotal amount, currency, exchangeRate FROM NextTransactionLineLink AS NTLL INNER JOIN Transaction AS NT ON (NT.ID = NTLL.NextDoc) WHERE NT.recordtype = 'vendorpayment'"
        if where_clauses:
            where_statement = " OR ".join(where_clauses)
            query += f" AND ({where_statement})"

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

        if not aggregate_payments:
            for payment in payments:
                if "internalid" in payment:
                    payment["internalId"] = payment.pop("internalid")
                if "externalid" in payment:
                    payment["externalId"] = payment.pop("externalid")
                if "tranid" in payment:
                    payment["tranId"] = payment.pop("tranid")

            return True, None, payments

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

        ns_account = self.config["ns_account"].replace("-", "_").upper()

        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        json_data = json.dumps(data, cls=HGJSONEncoder) if data else None

        res = requests.request(
            method=method,
            url=url,
            params=request_params,
            headers=request_headers,
            data=json_data,
            verify=True,
            auth=oauth
        )

        if res.status_code >= 400:
            self.logger.error(f"Error when making request: {res.request.method} {res.request.url} {res.request.body}: {res.status_code} {res.reason} {res.text}")

        return res

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
