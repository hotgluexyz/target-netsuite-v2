"""netsuite-v2 target sink class, which handles writing streams."""


from target_hotglue.sinks import HotglueSink
import requests
from oauthlib import oauth1
from requests_oauthlib import OAuth1
from pendulum import parse
import json
from lxml import etree


def get_clean_error_message(response: requests.models.Response) -> str:
    """Extract clean error message from NetSuite API response."""
    try:
        error_details = response.json().get("o:errorDetails", [])
        if error_details and len(error_details) > 0:
            detail = error_details[0].get("detail", "")
            if detail:
                return detail
        
        # Fallback to full JSON if we can't extract the detail
        return json.dumps(error_details)
    except (json.JSONDecodeError, AttributeError, KeyError):
        # Fallback if JSON parsing fails or response structure is unexpected
        return response.text

def validate_response(response: requests.models.Response) -> None:
    try:
        response.raise_for_status()
    except Exception as exc:
        clean_error = get_clean_error_message(response)
        raise Exception(f"Request to url {response.url} failed with response: {clean_error}") from exc


class netsuiteRestV2Sink(HotglueSink):
    """netsuite-v2 target sink class."""

    def _extract_id_from_response_header(self, headers):
        location = headers.get("Location")
        if not location:
            return None
        return location.split("/")[-1]

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return (
            f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/record/v1/"
        )

    @property
    def url_suiteql(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

    def rest_search(self, object, search, expand=False):
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json"}
        url = f"{self.url_base}{object}?q={search}"
        response = requests.get(url, headers=headers, auth=oauth)
        search_response = response.json()

        if expand:
            results = []

            for i in search_response.get("items", []):
                r = requests.get(i["links"][0]["href"], headers=headers, auth=oauth)
                results.append(r.json())

            return results

        return [r["id"] for r in search_response.get("items", [])]


    def rest_post(self, **kwarg):
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json"}
        response = requests.post(**kwarg, headers=headers, auth=oauth)
        if response.status_code >= 400:
            try:
                self.logger.error(f"INVALID PAYLOAD: {json.dumps(kwarg['json'])}")
                clean_error = get_clean_error_message(response)
                self.logger.error(f"NetSuite API Error: {clean_error}")
                validate_response(response)
            except:
                raise Exception(f"Request to url {kwarg['url']} failed with response: {response.text}")
        return response

    def rest_patch(self, **kwarg):
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json"}
        response = requests.patch(**kwarg, headers=headers, auth=oauth)
        self.logger.info(response.text)
        if response.status_code >= 400:
            try:
                clean_error = get_clean_error_message(response)
                self.logger.error(f"NetSuite API Error: {clean_error}")
                self.logger.error(f"INVALID PAYLOAD: {json.dumps(kwarg['json'])}")
                validate_response(response)
            except:
                validate_response(response)
        return response

    def process_order(self, context, record):
        sale_order = {}
        items = []
        matching_customers = []

        # Get the NetSuite Customer Ref
        if record.get("customer_id"):
            sale_order["entity"] = {"id": record.get("customer_id")}
        elif record.get("customer_name"):
            customer_name = record['customer_name']
            matching_customers = self.rest_search("customer", f'companyName IS "{customer_name}"')

            if len(matching_customers) == 0:
                first_name = customer_name.split(" ")[0]
                last_name = customer_name.split(" ")[-1]
                matching_customers = self.rest_search("customer", f'firstName CONTAIN "{first_name}" AND lastName CONTAIN "{last_name}"')

            if len(matching_customers) > 0:
                sale_order["entity"] = {"id": matching_customers[0]}

        trandate = record.get("transaction_date")
        if isinstance(trandate, str):
            trandate = parse(trandate)
        sale_order["tranDate"] = trandate.strftime("%Y-%m-%d")

        for line in record.get("line_items", []):
            order_item = {}

            # Get the product Id
            if line.get("product_id"):
                order_item["item"] = {"id": line.get("product_id")}

            elif line.get("product_name"):
                product_name = line.get("product_name").strip()
                
                matching_items = self.rest_search("inventoryItem", f'itemId IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("nonInventorySaleItem", f'itemId IS "{product_name}"')
            
                if len(matching_items) == 0:
                    matching_items = self.rest_search("nonInventorySaleItem", f'displayName IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("inventoryItem", f'displayName IS "{product_name}"')

                if len(matching_items) > 0:
                    order_item["item"] = {"id": matching_items[0]}
                
                elif len(matching_items) == 0 and product_name.isdigit():
                    order_item["item"] = {"id": product_name}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("amount", 1) * line.get("unit_price", 0)
            order_item["expected_ship_date"] = line.get("expectedShipDate")
            items.append(order_item)
        sale_order["item"] = {"items": items}
        # Get order number
        if record.get("id") is not None:
            sale_order["id"] = record.get("id")

        if record.get("order_number") is not None:
            sale_order["ref"] = record.get("order_number")
        
        sale_order["taxSchedule"] = record.get("tax_schedule", "1")
        
        if record.get("ship_date"):
            sale_order["shipDate"] = record.get("ship_date")
        
        if record.get("due_date"):
            sale_order["dueDate"] = record.get("due_date")
        
        if record.get('custom_fields'):
            record["custom_fields"] = json.loads(record["custom_fields"])
            for field in record["custom_fields"]:
                sale_order[field['name']] = field['value']
        
        if record.get('location_id'):
            sale_order["location"] = {"id": str(record.get('location_id'))}
        
        if not sale_order.get("location") and record.get('subsidiary_id') and self.reference_data.get("Locations"):
            for location in self.reference_data["Locations"]:
                for subsidiary in location.get("subsidiaryList", []):
                    if str(record.get('subsidiary_id')) == subsidiary["internalId"]:
                        sale_order["location"] = {"id": location["internalId"]}
        
        if record.get('subsidiary_id'):
            sale_order["subsidiary"] = {"id": str(record.get('subsidiary_id'))}
    
        if record.get('tax_schedule_id'):
            sale_order["taxSchedule"] = str(record.get('tax_schedule_id'))
        
        if record.get("custom_form_id"):
            sale_order["customForm"] = {"id": str(record["custom_form_id"])}

        return sale_order

    def process_vendor_bill(self, context, record):
        vendor_bill = {}

        if record.get("vendorBillNumber"):
            vendor_bill["externalId"] = record["vendorBillNumber"]
        elif record.get("invoiceNumber"):
            vendor_bill["externalId"] = record["invoiceNumber"]
        elif record.get("externalId"):
            vendor_bill["externalId"] = record["externalId"].get("value")

        vendor_bill["memo"] = record.get("description")

        if record.get("customFormId"):
            vendor_bill["customForm"] = {"id": record["customFormId"]}

        # Get the NetSuite Vendor Ref
        if record.get("vendorId") or record.get("vendorNum"):
            vendor_bill["entity"] = {
                "id": record.get("vendorId", record.get("vendorNum"))
            }
        elif record.get("vendorName"):
            vendor_name = record.get("vendorName")
            matching_vendors = self.rest_search("vendor", f'entityId IS "{vendor_name}"')

            if len(matching_vendors) > 0:
                vendor_bill["entity"] = {"id": matching_vendors[0]}

        # Prevent parse function from failing on empty date
        duedate = record.get("dueDate")
        if duedate:
            if isinstance(duedate, str):
                duedate = parse(duedate)
                vendor_bill["duedate"] = duedate.strftime("%Y-%m-%d")

        enddate = record.get("paidDate")
        if enddate:
            if isinstance(enddate, str):
                enddate = parse(enddate)
            if enddate:
                vendor_bill["enddate"] = enddate.strftime("%Y-%m-%d")

        # Get the NetSuite Location Ref
        location = None
        if record.get("locationId"):
            location = {"id": record["locationId"]}
        elif self.reference_data.get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in self.reference_data["Locations"]
                if l["name"] == record["location"]
            ]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}

        department = None
        if record.get("departmentId"):
            department = {"id": record["departmentId"]}
        elif self.reference_data.get("Departments") and record.get("department"):
            dep_data = [
                d
                for d in self.reference_data["Departments"]
                if d["name"] == record["department"]
            ]
            if dep_data:
                dep_data = dep_data[0]
                department = {"id": dep_data.get("internalId")}

        if location:
            vendor_bill["Location"] = location
        if department:
            vendor_bill["Department"] = department

        vendor_bill["tranid"] = record.get("invoiceNumber", record.get("number", record.get("vendorBillNumber")))

        startdate = record.get("issueDate", record.get("createdAt"))
        if isinstance(startdate, str):
            startdate = parse(startdate)
        vendor_bill["tranDate"] = startdate.strftime("%Y-%m-%d")

        # Get the NetSuite Subsidiary Ref
        if record.get("subsidiaryId"):
            vendor_bill["subsidiary"] = {"id": record.get("subsidiaryId")}
        if record.get("subsidiary"):
            subsidiary_name = record.get("subsidiary")
            matching_subs = self.rest_search("subsidiary", f'name IS "{subsidiary_name}"')

            if len(matching_subs) > 0:
                vendor_bill["subsidiary"] = {"id": matching_subs[0]}

        items = []
        for line in record.get("lineItems", []):
            order_item = {}

            if record.get("purchaseOrderNumber"):
                order_item["orderDoc"] = {"id": record["purchaseOrderNumber"]}

            order_item["description"] = line.get("description")

            # Get the product Id
            if line.get("productId"):
                order_item["item"] = {"id": line.get("productId")}
            elif line.get("productName"):
                product_name = line.get("productName")
                matching_items = self.rest_search("inventoryItem", f'itemId IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("inventoryItem", f'displayName IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("nonInventoryPurchaseItem", f'itemId IS "{product_name}"')

                if len(matching_items) > 0:
                    order_item["item"] = {"id": matching_items[0]}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = round(
                line.get("quantity") * line.get("unitPrice"), 3
            )

            if line.get("locationId"):
                order_item["Location"] = {"id": line.get("locationId")}
            if department:
                order_item["Department"] = department
            elif line.get("departmentId"):
                department = {"id": line["departmentId"]}
                order_item["Department"] = department
            elif self.reference_data.get("Departments") and line.get(
                "department"
            ):
                dep_data = [
                    d
                    for d in self.reference_data["Departments"]
                    if d["name"] == line["department"]
                ]
                if dep_data:
                    dep_data = dep_data[0]
                    department = {"id": dep_data.get("internalId")}
                    order_item["Department"] = department
            class_data = None
            if line.get("classId"):
                class_data = {"id": line["classId"]}
                order_item["Class"] = class_data

            items.append(order_item)
        if items:
            vendor_bill["item"] = {"items": items}

        expenses = []
        for line in record.get("expenses", []):
            expense = {}

            expense["memo"] = line.get("description")

            # Get the account Id
            if line.get("accountId"):
                expense["account"] = {"id": line.get("accountId")}
            elif self.reference_data.get("Accounts") and line.get(
                "accountNumber"
            ):
                acct_num = str(line["accountNumber"])
                acct_data = [
                    a
                    for a in self.reference_data["Accounts"]
                    if a["acctNumber"] == acct_num
                ]
                if acct_data:
                    self.logger.info(f"Account found for acctNumber {acct_num} -> {acct_data}")
                    acct_data = acct_data[0]
                    expense["account"] = {"id": acct_data.get("internalId")}
            expense["amount"] = round(line.get("amount"), 3)
            # Get the project id
            # project should be linked through customer HGI-6300
            if line.get("projectId"):
                expense["customer"] = {"id": line.get("projectId")}


            if line.get("customFields"):
                for field in line.get("customFields"):
                    expense[field["name"]] = field["value"]

            # Get the NetSuite Location Ref
            location = None
            if line.get("locationId"):
                location = {"id": line["locationId"]}
            elif self.reference_data.get("Locations") and line.get("location"):
                loc_data = [
                    l
                    for l in self.reference_data["Locations"]
                    if l["name"] == line["location"]
                ]
                if loc_data:
                    loc_data = loc_data[0]
                    location = {"id": loc_data.get("internalId")}

            if location:
                expense["Location"] = location

            if department:
                expense["Department"] = department
            expenses.append(expense)
        if expenses:
            vendor_bill["expense"] = {"items": expenses}

        return vendor_bill

    def process_invoice(self, context, record):
        invoice = {}
        items = []
        if record.get("invoiceNumber"):
            invoice["tranId"] = record["invoiceNumber"]

        # Get the NetSuite Customer Ref
        if record.get("customerName"):
            customer_name = record['customerName']
            matching_customers = self.rest_search("customer", f'companyName IS "{customer_name}"')

            if len(matching_customers) == 0:
                first_name = customer_name.split(" ")[0]
                last_name = customer_name.split(" ")[-1]
                matching_customers = self.rest_search("customer", f'firstName CONTAIN "{first_name}" AND lastName CONTAIN "{last_name}"')

            if len(matching_customers) > 0:
                invoice["entity"] = {"id": matching_customers[0]}

        # Get the NetSuite Location Ref
        if self.reference_data.get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in self.reference_data["Locations"]
                if l["name"] == record["location"]
            ]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}
        else:
            location = {"id": record.get("locationId", "2")}

        invoice["Location"] = location

        # Get the NetSuite Subsidiary Ref
        if record.get("subsidiary"):
            subsidiary_name = record.get("subsidiary")
            matching_subs = self.rest_search("subsidiary", f'name IS "{subsidiary_name}"')

            if len(matching_subs) > 0:
                invoice["Subsidiary"] = {"id": matching_subs[0]}

        duedate = record.get("dueDate")
        if isinstance(duedate, str):
            duedate = parse(duedate)
            invoice["duedate"] = duedate.strftime("%Y-%m-%d")

        enddate = record.get("paidDate")
        if isinstance(enddate, str):
            enddate = parse(enddate)
        if enddate:
            invoice["enddate"] = enddate.strftime("%Y-%m-%d")

        startdate = record.get("issueDate")
        if isinstance(startdate, str):
            startdate = parse(startdate)
            invoice["startdate"] = startdate.strftime("%Y-%m-%d")
        for line in record.get("lineItems", []):
            order_item = {}

            # Get the product Id
            if "productId" in line:
                order_item["item"] = {"id": line["productId"]}
            elif line.get("productName"):
                product_name = line.get("productName")
                matching_items = self.rest_search("inventoryItem", f'itemId IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("inventoryItem", f'displayName IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("nonInventorySaleItem", f'itemId IS "{product_name}"')

                if len(matching_items) > 0:
                    order_item["item"] = {"id": matching_items[0]}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("quantity") * line.get("unitPrice")
            order_item["Location"] = location
            items.append(order_item)
        invoice["item"] = {"items": items}
        return invoice

    def invoice_payment(self, context, record):
        invoice_id = record.get("invoice_id")
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2017_2"

        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2017_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soap:Header>
        <tokenPassport>
            <account>{oauth_creds["account"]}</account>
            <consumerKey>{oauth_creds["consumerKey"]}</consumerKey>
            <token>{oauth_creds["token"]}</token>
            <nonce>{oauth_creds["nonce"]}</nonce>
            <timestamp>{oauth_creds["timestamp"]}</timestamp>
            <signature algorithm="HMAC-SHA256">{oauth_creds["signature"]["_value_1"]}</signature>
        </tokenPassport>
    </soap:Header>
    <soap:Body>
        <platformMsgs:initialize xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:platformCoreTyp="urn:types.core_2017_2.platform.webservices.netsuite.com" xmlns:platformCore="urn:core_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com">
            <platformMsgs:initializeRecord>
                <platformCore:type>customerPayment</platformCore:type>
                <platformCore:reference internalId="{invoice_id}" type="invoice">
                </platformCore:reference>
            </platformMsgs:initializeRecord>
        </platformMsgs:initialize>
    </soap:Body>
</soap:Envelope>"""

        headers = {"SOAPAction": "initialize", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        res_xml = etree.fromstring(res.text.encode())
        record = res_xml[1][0][0][-1]

        for r in record:
            if isinstance(r.text, str):
                r.getparent().remove(r)

        return etree.tostring(record, pretty_print=True)

    def vendor_payment(self, context, record):
        vendor_bill_id = record.get("id")
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2017_2"

        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2017_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soap:Header>
        <tokenPassport>
            <account>{oauth_creds["account"]}</account>
            <consumerKey>{oauth_creds["consumerKey"]}</consumerKey>
            <token>{oauth_creds["token"]}</token>
            <nonce>{oauth_creds["nonce"]}</nonce>
            <timestamp>{oauth_creds["timestamp"]}</timestamp>
            <signature algorithm="HMAC-SHA256">{oauth_creds["signature"]["_value_1"]}</signature>
        </tokenPassport>
    </soap:Header>
    <soap:Body>
        <platformMsgs:initialize xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:platformCoreTyp="urn:types.core_2017_2.platform.webservices.netsuite.com" xmlns:platformCore="urn:core_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com">
            <platformMsgs:initializeRecord>
                <platformCore:type>vendorPayment</platformCore:type>
                <platformCore:reference internalId="{vendor_bill_id}" type="vendorBill">
                </platformCore:reference>
            </platformMsgs:initializeRecord>
        </platformMsgs:initialize>
    </soap:Body>
</soap:Envelope>"""

        headers = {"SOAPAction": "initialize", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        res_xml = etree.fromstring(res.text.encode())
        record = res_xml[1][0][0][-1]

        for r in record:
            if isinstance(r.text, str):
                r.getparent().remove(r)

        return etree.tostring(record, pretty_print=True)

    def push_vendor_payments(self, payload):
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2017_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        payload = payload.decode()
        payload = "\n".join(payload.split("\n")[1:-2])

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2017_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <soap:Header>
                <tokenPassport>
                    <account>{oauth_creds["account"]}</account>
                    <consumerKey>{oauth_creds["consumerKey"]}</consumerKey>
                    <token>{oauth_creds["token"]}</token>
                    <nonce>{oauth_creds["nonce"]}</nonce>
                    <timestamp>{oauth_creds["timestamp"]}</timestamp>
                    <signature algorithm="HMAC-SHA256">{oauth_creds["signature"]["_value_1"]}</signature>
                </tokenPassport>
            </soap:Header>
            <soap:Body>
                <platformMsgs:add>
                <platformMsgs:record xsi:type="tranCust:VendorPayment" xmlns:tranCust="urn:vendors_2017_2.transactions.webservices.netsuite.com">
                    {payload}
                </platformMsgs:record>
                </platformMsgs:add>
            </soap:Body>
        </soap:Envelope>"""

        headers = {"SOAPAction": "add", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        return res

    def push_payments(self, payload):
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2017_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        payload = payload.decode()
        payload = "\n".join(payload.split("\n")[1:-2])

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2017_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <soap:Header>
                <tokenPassport>
                    <account>{oauth_creds["account"]}</account>
                    <consumerKey>{oauth_creds["consumerKey"]}</consumerKey>
                    <token>{oauth_creds["token"]}</token>
                    <nonce>{oauth_creds["nonce"]}</nonce>
                    <timestamp>{oauth_creds["timestamp"]}</timestamp>
                    <signature algorithm="HMAC-SHA256">{oauth_creds["signature"]["_value_1"]}</signature>
                </tokenPassport>
            </soap:Header>
            <soap:Body>
                <platformMsgs:add>
                <platformMsgs:record xsi:type="tranCust:CustomerPayment" xmlns:tranCust="urn:customers_2017_2.transactions.webservices.netsuite.com">
                    {payload}
                </platformMsgs:record>
                </platformMsgs:add>
            </soap:Body>
        </soap:Envelope>"""

        headers = {"SOAPAction": "add", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        return res

    def po_to_vb(self, payload):
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2017_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        po_number = payload["poNumber"]

        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        response = self.ns_client.entities["PurchaseOrder"].get_all(
            ["entity", "location"], tran_id=po_number
        )[0]
        po_id = response.get("internalId")

        entity_id = response["entity"]["internalId"]
        location_id = response["location"]["internalId"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2017_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2017_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2017_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <soap:Header>
                <tokenPassport>
                    <account>{oauth_creds["account"]}</account>
                    <consumerKey>{oauth_creds["consumerKey"]}</consumerKey>
                    <token>{oauth_creds["token"]}</token>
                    <nonce>{oauth_creds["nonce"]}</nonce>
                    <timestamp>{oauth_creds["timestamp"]}</timestamp>
                    <signature algorithm="HMAC-SHA256">{oauth_creds["signature"]["_value_1"]}</signature>
                </tokenPassport>
            </soap:Header>
            <soap:Body>  
               <add xmlns="urn:messages_2017_2.platform.webservices.netsuite.com">   
                  <record xsi:type="ns6:VendorBill" xmlns:ns6="urn:purchases_2017_2.transactions.webservices.netsuite.com">    
                     <ns6:entity internalId="{entity_id}" xsi:type="ns7:RecordRef" xmlns:ns7="urn:core_2017_2.platform.webservices.netsuite.com"/>
                     <ns6:location internalId="{location_id}" xsi:type="ns7:RecordRef" xmlns:ns7="urn:core_2017_2.platform.webservices.netsuite.com"/> 
                     <ns6:purchaseOrderList xsi:type="ns8:RecordRefList" xmlns:ns8="urn:core_2017_2.platform.webservices.netsuite.com">     
                        <ns8:recordRef internalId="{po_id}" type="purchaseOrder" xsi:type="ns8:RecordRef"/>
                     </ns6:purchaseOrderList>   
                  </record>  
               </add> 
            </soap:Body> 
        </soap:Envelope>"""

        headers = {"SOAPAction": "add", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        return res

    def process_customer(self, context, record):
        subsidiary = record.get("subsidiary")
        contact_name = record.get("contactName", None)

        if contact_name is None and record.get("companyName"):
            contact_name = record.get("companyName")
        
        if contact_name is None and record.get("firstName"):
            contact_name = f'{record.get("firstName")} {record.get("lastName")}'

        if contact_name:
            names = contact_name.split(" ")
            if len(names) > 0:
                first_name = names[0]
                last_name = " ".join(names[1:])
            else:
                first_name = names[0]
                last_name = (" ",)

        address_book = [
            {
                "addressBookAddress": {
                    "addr1": address.get("line1"),
                    "addr2": address.get("line2"),
                    "addr3": address.get("line3"),
                    "city": address.get("city"),
                    "state": address.get("state"),
                    "zip": address.get("postalCode"),
                    "country": address.get("country"),
                }
            }
            for address in record.get("addresses", [])
        ]

        address = record.get("addresses")
        customer = {
            "companyName": record.get("customerName"),
            "firstName": first_name,
            "lastName": last_name,
            "email": record.get("emailAddress"),
            "phone": record.get("phoneNumbers")[0].get("number")
            if record.get("phoneNumbers")
            else None,
            "comments": record.get("notes"),
            "balance": record.get("balance"),
            "datecreated": record.get("createdAt"),
            "taxable": record.get("taxable"),
            "isInactive": not record.get("active"),
            "addressbook": {"items": address_book},
            "defaultAddress": f"{address[0].get('line1')} {address[0].get('line2')} {address[0].get('line3')}, {address[0].get('city')} {address[0].get('postalCode')}, {address[0].get('state')}, {address[0].get('country')}"
            if address
            else None,
        }

        if subsidiary:
            customer["subsidiary"] = {"id": subsidiary}
        else:
            customer["subsidiary"] = {"id": 1}

        return customer

    def process_credit_memo(self, context, record):
        return record

    def process_vendors(self, context, record):
        vendor = None
        if record.get("id"):
            vendor_id = record.get("id")
            matching_vendors = self.rest_search("vendor", f'externalId IS "{vendor_id}"', expand=True)

            if len(matching_vendors) == 0:
                matching_vendors = self.rest_search("vendor", f'id EQUAL "{vendor_id}"', expand=True)

            if len(matching_vendors) > 0:
                vendor = matching_vendors[0]

        address = record.get("addresses")
        phoneNumber = record.get("phoneNumbers")
        vendor_mapping = {
            "email": record.get("emailAddress"),
            "companyName": record.get("vendorName"),
            "dateCreated": record.get("createdAt"),
            "entityId": record.get("vendorName"),
            "firstName": record.get("contactName"),
            "subsidiary": {"id": record.get("subsidiary")},
            "lastModifiedDate": record.get("updatedAt"),
            "currency": {"refName": record.get("currency")},
            "homePhone": phoneNumber[0]["number"] if phoneNumber else None,
            "defaultAddress": f"{address[0]['line1']} {address[0]['line2']} {address[0]['line3']}, {address[0]['city']}, {address[0]['state'], address[0]['country'], address[0]['postalCode']}"
            if address
            else None,
        }

        if vendor:
            vendor_mapping["internalId"] = vendor.get("internalId")
            vendor_mapping["accountNumber"] = vendor.get("accountNumber")

        return vendor_mapping

    def process_item(self, context, record):
        def get_account_by_name_or_id(x, accountName, id):
            if accountName:
                return x["acctName"] == accountName
            elif id:
                return x["internalId"] == id
            else:
                return False

        payload = {
            "displayName": record.get("name"),
            "createdAt": record.get("createdAt"),
            "reorderPoint": record.get("reorderPoint"),
            "upcCode": record.get("code"),
            "quantityOnHand": record.get("quantityOnHand"),
            "isInactive": not record.get("active"),
            "itemId": record.get("name"),
        }

        if record.get("isBillItem"):
            cogsAccount = json.loads(record.get("billItem"))
            cost = cogsAccount.get("unitPrice")
            accountName = cogsAccount.get("accountName")
            id = cogsAccount.get("accountId")
            account = list(
                filter(
                    lambda x: get_account_by_name_or_id(x, accountName, id),
                    self.reference_data["Accounts"],
                )
            )[0]
            payload["cogsAccount"] = {"id": account["internalId"]}
            payload["cost"] = cost

        if record.get("isInvoiceItem"):
            invoiceAccount = json.loads(record.get("invoiceItem"))
            price = invoiceAccount["unitPrice"]
            accountName = invoiceAccount.get("accountName")
            id = invoiceAccount.get("accountId")
            if accountName or id:
                account = list(
                    filter(
                        lambda x: get_account_by_name_or_id(x, accountName, id),
                        self.reference_data["Accounts"],
                    )
                )[0]
                payload["incomeAccount"] = {"id": account["internalId"]}

        return payload

    def process_purchase_order(self, context, record):
        purchase_order = {}
        if record.get("order_number"):
            purchase_order['externalId'] = record['order_number']
        elif record.get("invoiceNumber"):
            purchase_order["externalId"] = record["invoiceNumber"]
        elif record.get("externalId"):
            purchase_order["externalId"] = record["externalId"].get("value")
        
        purchase_order["memo"] = record.get("description")

        if record.get("customFormId"):
            purchase_order["customForm"] = {"id": record["customFormId"]}

        # Get the NetSuite Vendor Ref
        if record.get("vendorId"):
            purchase_order["entity"] = {"id": record.get("vendorId")}
        elif record.get("vendorName"):
            vendor_name = record.get("vendorName")
            matching_vendors = self.rest_search("vendor", f'entityId IS "{vendor_name}"')

            if len(matching_vendors) > 0:
                purchase_order["entity"] = {"id": matching_vendors[0]}

        #Prevent parse function from failing on empty date
        duedate = record.get("dueDate")
        if duedate:
            if isinstance(duedate, str):
                duedate = parse(duedate)
                purchase_order["duedate"] = duedate.strftime("%Y-%m-%d")
        
        enddate = record.get("paidDate")
        if enddate:
            if isinstance(enddate, str):
                enddate = parse(enddate)
            if enddate:
                purchase_order["endDate"] = enddate.strftime("%Y-%m-%d")
        
        # Get the NetSuite Location Ref
        location = None
        if record.get("locationId"):
            location = {"id": record["locationId"]}
        elif self.reference_data.get("Locations") and record.get("location"):
            loc_data = [l for l in self.reference_data["Locations"] if l["name"] == record["location"]]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}

        if location:
            purchase_order["Location"] = location
        purchase_order["tranid"] = record.get("order_number")

        items = []
        for line in record.get("line_items", []):
            order_item = {}

            if record.get("order_number"):
                order_item["orderDoc"] = {"id": record["order_number"]}

            order_item["description"] = line.get("description")
            
            # Get the product Id
            if line.get("product_id"):
                order_item["item"] = {"id": line.get("product_id")}
            elif line.get("product_name"):
                product_name = line.get("product_name")
                matching_items = self.rest_search("inventoryItem", f'itemId IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("inventoryItem", f'displayName IS "{product_name}"')

                if len(matching_items) == 0:
                    matching_items = self.rest_search("nonInventorySaleItem", f'itemId IS "{product_name}"')

                if len(matching_items) > 0:
                    order_item["item"] = {"id": matching_items[0]}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = round(line.get("quantity") * line.get("unit_price"), 3)
             
            items.append(order_item)
        if items:
            purchase_order["item"] = {"items": items}
            
        return purchase_order

