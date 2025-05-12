"""netsuite-v2 target sink class, which handles writing streams."""

from singer_sdk.sinks import BatchSink
import requests
from oauthlib import oauth1
from requests_oauthlib import OAuth1
from pendulum import parse
import json
from lxml import etree
import ast


class netsuiteRestV2Sink(BatchSink):
    """netsuite-v2 target sink class."""

    @property
    def url_account(self) -> str:
        return self.config["ns_account"].replace("_", "-").replace("SB", "sb")

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return (
            f"https://{self.url_account}.suitetalk.api.netsuite.com/services/rest/record/v1/"
        )

    @property
    def url_suiteql(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"https://{self.url_account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

    def rest_post(self, **kwarg):
        ns_account = self.config["ns_account"].replace("-", "_").upper()
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json", "Prefer": "transient"}
        response = requests.post(**kwarg, headers=headers, auth=oauth)
        if response.status_code >= 400:
            try:
                self.logger.error(f"Failed request payload: {json.dumps(kwarg['json'])}")
                self.logger.error(json.dumps(response.json().get("o:errorDetails")))
                response.raise_for_status()
            except:
                response.raise_for_status()
        return response

    def rest_get(self, **kwarg):
        ns_account = self.config["ns_account"].replace("-", "_").upper()
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json"}
        response = requests.get(**kwarg, headers=headers, auth=oauth)
        if response.status_code >= 400:
            try:
                self.logger.error(json.dumps(response.json().get("o:errorDetails")))
                response.raise_for_status()
            except:
                response.raise_for_status()
        return response

    def rest_patch(self, **kwarg):
        ns_account = self.config["ns_account"].replace("-", "_").upper()
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        headers = {"Content-Type": "application/json"}
        response = requests.patch(**kwarg, headers=headers, auth=oauth)
        self.logger.info(response.text)
        if response.status_code >= 400:
            try:
                self.logger.error(json.dumps(response.json().get("o:errorDetails")))
                self.logger.error(f"INVALID PAYLOAD: {json.dumps(kwarg['json'])}")
                response.raise_for_status()
            except:
                response.raise_for_status()
        return response
    

    def check_custom_field(self, script_id):
        # validates if a custom field is valid using SuiteQL
        try:
            url = self.url_base.replace("/rest/record/v1/", "/rest/query/v1/suiteql?limit=1000")
            custom_fields = self.rest_post(url=url, json={
                "q": f"SELECT id, scriptid, name, recordtype FROM customfield WHERE scriptid='{script_id}'"
            }).json()
            return custom_fields.get("count") > 0
        except:
            self.logger.exception(f"Failed to check existence of custom field '{script_id}'. Assuming it exists.")
            return True

    def parse_objs(self, record):
        if isinstance(record, str):
            try:
                return json.loads(record)
            except:
                try:
                    return ast.literal_eval(record)
                except:
                    return record
        return record
    
    def get_account_by_name_id_number(self, x, accountName, id, number=None):
        return (
            (id is not None and x["internalId"] == id) or
            (number is not None and x["acctNumber"] == number) or
            (accountName is not None and x["acctName"] == accountName)
        )

    def process_order(self, context, record):
        sale_order = {}
        items = []

        # Get the NetSuite Customer Ref
        if context["reference_data"].get("Customer") and record.get("customer_name"):
            customer_names = []
            for c in context["reference_data"]["Customer"]:
                if "name" in c.keys():
                    if c["name"]:
                        customer_names.append(c["name"])
                else:
                    if c["companyName"]:
                        customer_names.append(c["companyName"])
            customer_name = self.get_close_matches(
                record["customer_name"], customer_names, n=2, cutoff=0.95
            )
            if customer_name:
                customer_name = max(customer_name, key=customer_name.get)
                customer_data = []
                for c in context["reference_data"]["Customer"]:
                    if "name" in c.keys():
                        if c["name"] == customer_name:
                            customer_data.append(c)
                    else:
                        if c["companyName"] == customer_name:
                            customer_data.append(c)
                if customer_data:
                    customer_data = customer_data[0]
                    sale_order["entity"] = {"id": customer_data.get("internalId")}

        trandate = record.get("transaction_date")
        if isinstance(trandate, str):
            trandate = parse(trandate)
        sale_order["tranDate"] = trandate.strftime("%Y-%m-%d")
        for line in record.get("line_items", []):
            order_item = {}

            # Get the product Id
            if context["reference_data"].get("Items") and line.get("product_name"):
                product_names = [
                    c["itemId"] for c in context["reference_data"]["Items"]
                ]
                product_name = self.get_close_matches(
                    line["product_name"], product_names, n=2, cutoff=0.95
                )
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [
                        c
                        for c in context["reference_data"]["Items"]
                        if c["itemId"] == product_name
                    ]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("unit_price")
            items.append(order_item)
        sale_order["item"] = {"items": items}
        # Get order number
        if record.get("order_number") is not None:
            sale_order["order_number"] = record.get("order_number")
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
        elif context["reference_data"].get("Vendors") and record.get("vendorName"):
            vendor_names = []
            for c in context["reference_data"]["Vendors"]:
                if "entityId" in c.keys():
                    vendor_names.append(c["entityId"])
            vendor_name = self.get_close_matches(
                record["vendorName"], vendor_names, n=2, cutoff=0.9
            )
            if vendor_name:
                vendor_name = max(vendor_name, key=vendor_name.get)
                vendor_data = []
                for c in context["reference_data"]["Vendors"]:
                    if c["entityId"] == vendor_name:
                        vendor_data.append(c)
                if vendor_data:
                    vendor_data = vendor_data[0]
                    vendor_bill["entity"] = {"id": vendor_data.get("internalId")}
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
        elif context["reference_data"].get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in context["reference_data"]["Locations"]
                if l["name"] == record["location"]
            ]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}

        department = None
        if record.get("departmentId"):
            department = {"id": record["departmentId"]}
        elif context["reference_data"].get("Departments") and record.get("department"):
            dep_data = [
                d
                for d in context["reference_data"]["Departments"]
                if d["name"] == record["department"]
            ]
            if dep_data:
                dep_data = dep_data[0]
                department = {"id": dep_data.get("internalId")}

        if location:
            vendor_bill["Location"] = location
        if department:
            vendor_bill["Department"] = department

        vendor_bill["tranid"] = record.get("invoiceNumber", record.get("number"))

        startdate = record.get("issueDate", record.get("createdAt"))
        if startdate:
            if isinstance(startdate, str):
                startdate = parse(startdate)
            vendor_bill["tranDate"] = startdate.strftime("%Y-%m-%d")

        # Get the NetSuite Subsidiary Ref
        if record.get("subsidiaryId"):
            vendor_bill["subsidiary"] = {"id": record.get("subsidiaryId")}
        if context["reference_data"].get("Subsidiaries") and record.get("subsidiary"):
            sub_data = [
                s
                for s in context["reference_data"]["Subsidiaries"]
                if s["name"] == record["subsidiary"]
            ]
            if sub_data:
                sub_data = sub_data[0]
                vendor_bill["subsidiary"] = {"id": sub_data.get("internalId")}

        items = []
        for line in record.get("lineItems", []):
            order_item = {}

            if record.get("purchaseOrderNumber"):
                order_item["orderDoc"] = {"id": record["purchaseOrderNumber"]}

            order_item["description"] = line.get("description")

            # Get the product Id
            if line.get("productId"):
                order_item["item"] = {"id": line.get("productId")}
            elif context["reference_data"].get("Items") and line.get("productName"):
                product_names = [
                    c["itemId"] for c in context["reference_data"]["Items"]
                ]
                product_name = self.get_close_matches(
                    line["productName"], product_names, n=2, cutoff=0.95
                )
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [
                        c
                        for c in context["reference_data"]["Items"]
                        if c["itemId"] == product_name
                    ]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}
            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = round(
                line.get("quantity") * line.get("unitPrice"), 3
            )
            if department:
                order_item["Department"] = department
            elif line.get("departmentId"):
                department = {"id": line["departmentId"]}
                order_item["Department"] = department
            elif context["reference_data"].get("Departments") and line.get(
                "department"
            ):
                dep_data = [
                    d
                    for d in context["reference_data"]["Departments"]
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
            elif context["reference_data"].get("Accounts") and line.get(
                "accountNumber"
            ):
                acct_num = str(line["accountNumber"])
                acct_data = [
                    a
                    for a in context["reference_data"]["Accounts"]
                    if a["acctNumber"] == acct_num
                ]
                if acct_data:
                    acct_data = acct_data[0]
                    expense["account"] = {"id": acct_data.get("internalId")}
            expense["amount"] = round(line.get("amount"), 3)

            if line.get("customFields"):
                for field in line.get("customFields"):
                    expense[field["name"]] = field["value"]

            # Get the NetSuite Location Ref
            location = None
            if line.get("locationId"):
                location = {"id": line["locationId"]}
            elif context["reference_data"].get("Locations") and line.get("location"):
                loc_data = [
                    l
                    for l in context["reference_data"]["Locations"]
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
        if context["reference_data"].get("Customer"):
            customer_data = []
            if record.get("customerId"):
                customer_data = [c for c in context["reference_data"]["Customer"] if c["internalId"] == record["customerId"]]
            if record.get("customerName") and not customer_data:
                customer_names = []
                for c in context["reference_data"]["Customer"]:
                    if "name" in c.keys():
                        if c["name"]:
                            customer_names.append(c["name"])
                    else:
                        if c["companyName"]:
                            customer_names.append(c["companyName"])
                customer_name = self.get_close_matches(
                    record["customerName"], customer_names, n=2, cutoff=0.5
                )
                if customer_name:
                    customer_name = max(customer_name, key=customer_name.get)
                    customer_data = []
                    for c in context["reference_data"]["Customer"]:
                        if "name" in c.keys():
                            if c["name"] == customer_name:
                                customer_data.append(c)
                        else:
                            if c["companyName"] == customer_name:
                                customer_data.append(c)
            if customer_data:
                customer_data = customer_data[0]
                invoice["entity"] = {"id": customer_data.get("internalId")}

        # Get the NetSuite Location Ref
        if context["reference_data"].get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in context["reference_data"]["Locations"]
                if l["name"] == record["location"]
            ]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}
        else:
            location = {"id": record["locationId"]} if record.get("locationId") else None
        if location:
            invoice["Location"] = location

        # Get the NetSuite Subsidiary Ref
        if context["reference_data"].get("Subsidiaries") and record.get("subsidiary"):
            # look for subsidiary id match 
            record["subsidiary"] = record["subsidiary"] if isinstance(record["subsidiary"], str) else str(record["subsidiary"]).split(".")[0]
            sub_data = [
                s
                for s in context["reference_data"]["Subsidiaries"]
                if s["internalId"] == record["subsidiary"]
            ]
            # look for subsidiary name
            if not sub_data:
                sub_data = [
                    s
                    for s in context["reference_data"]["Subsidiaries"]
                    if s["name"] == record["subsidiary"]
                ]
            if sub_data:
                sub_data = sub_data[0]
                invoice["Subsidiary"] = {"id": sub_data.get("internalId")}

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
            invoice["tranDate"] = startdate.strftime("%Y-%m-%d")
        
        if record.get("currency"):
            invoice["currency"] = {"refName": record["currency"]}

        for line in record.get("lineItems", []):
            order_item = {}

            # Get the product Id
            if "productId" in line:
                order_item["item"] = {"id": line["productId"]}
            elif context["reference_data"].get("Items") and line.get("productName"):
                product_names = [
                    c["itemId"] for c in context["reference_data"]["Items"]
                ]
                product_name = self.get_close_matches(
                    line["productName"], product_names, n=2, cutoff=0.95
                )
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [
                        c
                        for c in context["reference_data"]["Items"]
                        if c["itemId"] == product_name
                    ]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("totalPrice", line.get("quantity") * line.get("unitPrice"))
            if location:
                order_item["Location"] = location
            items.append(order_item)
        invoice["item"] = {"items": items}
        return invoice

    def invoice_payment(self, context, record):
        raw_record = record.copy()
        invoice_id = record.get("transactionId", record.get("id"))
        url = f"https://{self.url_account}.suitetalk.api.netsuite.com/services/NetSuitePort_2024_2"

        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2024_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
                <platformMsgs:initialize xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:platformCoreTyp="urn:types.core_2024_2.platform.webservices.netsuite.com" xmlns:platformCore="urn:core_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com">
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
        
        # Add fields at the header level with tranCust: namespace prefix
        if raw_record.get("amount"):
            payment_elem = etree.Element("{urn:customers_2024_2.transactions.webservices.netsuite.com}payment")
            payment_elem.text = str(raw_record["amount"])
            record.append(payment_elem)
            
            # Find the applyList element
            apply_list = record.find(".//{urn:customers_2024_2.transactions.webservices.netsuite.com}applyList")
            if apply_list is not None:
                # Find the apply element with value "true"
                for apply_elem in apply_list.findall(".//{urn:customers_2024_2.transactions.webservices.netsuite.com}apply"):
                    apply_value = apply_elem.find(".//{urn:customers_2024_2.transactions.webservices.netsuite.com}apply")
                    if apply_value is not None and apply_value.text == "true":
                        # Update the amount in this apply element
                        amount_elem = apply_elem.find(".//{urn:customers_2024_2.transactions.webservices.netsuite.com}amount")
                        if amount_elem is not None:
                            amount_elem.text = str(raw_record["amount"])
                        break
        
        if raw_record.get("date"):
            tran_date_elem = etree.Element("{urn:customers_2024_2.transactions.webservices.netsuite.com}tranDate")
            tran_date_elem.text = raw_record["date"]
            record.append(tran_date_elem)

        # field araccount uses the same account as the invoice and it's read-only
        # field account is the bank account that will be used to pay the invoice, it can only be passed if funds have been already deposited
        if context["reference_data"].get("Accounts"):
            acct_data = None
            if raw_record.get("accountNumber"):
                acct_num = str(raw_record["accountNumber"])
                acct_data = [
                    a
                    for a in context["reference_data"]["Accounts"]
                    if a["acctNumber"] == acct_num
                ]
            if not acct_data and raw_record.get("accountName"):
                acct_name = raw_record["accountName"]
                acct_data = [
                    a
                    for a in context["reference_data"]["Accounts"]
                    if a["acctName"] == acct_name
                ]
            if acct_data:
                acct_data = acct_data[0]
                account_elem = etree.Element(
                    "{urn:customers_2024_2.transactions.webservices.netsuite.com}account",
                    attrib={"internalId": acct_data["internalId"]},
                )
                record.append(account_elem)
                undep_funds_elem = etree.Element(
                    "{urn:customers_2024_2.transactions.webservices.netsuite.com}undepFunds"
                )
                undep_funds_elem.text = "false"
                record.append(undep_funds_elem)
        
        if context["reference_data"].get("Currencies") and raw_record.get("currency"):
            currency_symbol = raw_record.get("currency")
            currency = [
                c
                for c in context["reference_data"]["Currencies"]
                if c["symbol"] == currency_symbol
            ]
            if currency:
                currency_elem = record.find(".//{urn:customers_2024_2.transactions.webservices.netsuite.com}currency")
                if currency_elem is not None:
                    name_elem = currency_elem.find("{urn:core_2024_2.platform.webservices.netsuite.com}name")
                    if name_elem is not None:
                        name_elem.text = currency[0].get("name")

        return etree.tostring(record, pretty_print=True)

    def vendor_payment(self, context, record):
        """
        Initialize a Vendor Payment:
        The initialize operation in NetSuite is used to create a new record (in this case, a vendorPayment)
        that is prepopulated with data from an existing record (in this case, a vendorBill).
        """
        vendor_bill_id = record.get("transactionId", record.get("id"))
        url = f"https://{self.url_account}.suitetalk.api.netsuite.com/services/NetSuitePort_2024_2"

        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2024_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
        <platformMsgs:initialize xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:platformCoreTyp="urn:types.core_2024_2.platform.webservices.netsuite.com" xmlns:platformCore="urn:core_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com">
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

        return {
            "payload": etree.tostring(record, pretty_print=True),
            "bill_id": vendor_bill_id
        }

    def process_vendor_credit(self, context, record):
        # Get the NetSuite Vendor Ref
        vendor_credit = {}
        if record.get("vendorId") or record.get("vendorNum"):
            vendor_credit["entity"] = {
                "id": record.get("vendorId", record.get("vendorNum"))
            }
        elif context["reference_data"].get("Vendors") and record.get("vendorName"):
            vendor_names = []
            for c in context["reference_data"]["Vendors"]:
                if "entityId" in c.keys():
                    vendor_names.append(c["entityId"])
            vendor_name = self.get_close_matches(
                record["vendorName"], vendor_names, n=2, cutoff=0.9
            )
            if vendor_name:
                vendor_name = max(vendor_name, key=vendor_name.get)
                vendor_data = []
                for c in context["reference_data"]["Vendors"]:
                    if c["entityId"] == vendor_name:
                        vendor_data.append(c)
                if vendor_data:
                    vendor_data = vendor_data[0]
                    vendor_credit["entity"] = {"id": vendor_data.get("internalId")}

        transaction_date = record.get("transactionDate")

        if transaction_date:
            if isinstance(transaction_date, str):
                transaction_date = parse(transaction_date)
            vendor_credit["tranDate"] = transaction_date.strftime("%Y-%m-%d")

        if record.get("currency"):
            vendor_credit["currency"] = {"refName": record.get("currency")}

        if record.get("amount"):
            vendor_credit["total"] = record.get("amount")
            vendor_credit["userTotal"] = record.get("amount")

        location = None
        if record.get("locationId"):
            location = {"id": record["locationId"]}
        elif context["reference_data"].get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in context["reference_data"]["Locations"]
                if l["name"] == record["location"]
            ]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}

        if location:
            vendor_credit["Location"] = location

        vendor_credit["item"] = {"items": []}
        if record.get("lineItems"):
            for item in record.get("lineItems", []):
                item_credit = {
                    "amount": record.get("amount")
                }
                if item.get("productId"):
                    item_credit["item"] = {"id": item.get("productId")}

                if item.get("productName") and item_credit.get("item") is None:
                    product_names = [
                        c["itemId"] for c in context["reference_data"]["Items"]
                    ]
                    product_name = self.get_close_matches(
                        item["productName"], product_names, n=2, cutoff=0.95
                    )
                    if product_name:
                        product_name = max(product_name, key=product_name.get)
                        product_data = [
                            c
                            for c in context["reference_data"]["Items"]
                            if c["itemId"] == product_name
                        ]
                        if product_data:
                            product_data = product_data[0]
                            item_credit["item"] = {"id": product_data.get("internalId")}
                        else:
                            item_credit["item"] = {"refName": item.get("productName")}

                vendor_credit["item"]["items"].append(item_credit)

        return vendor_credit

    def push_vendor_payments(self, record):
        """
        pushes a new VendorPayment record to NetSuite.
        """
        payload = record['payload']
        bill_id = record['bill_id']
        bill_obj = self.rest_get(url=f"{self.url_base}vendorBill/{bill_id}").json()
        if bill_obj.get("balance") == 0:
            raise Exception(f"Bill with id={bill_id} has already been paid in full!")
        url = f"https://{self.url_account}.suitetalk.api.netsuite.com/services/NetSuitePort_2024_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        payload = payload.decode()
        payload = "\n".join(payload.split("\n")[1:-2])

        # TODO: we might not need the applyList part if the balance is >0
        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2024_2.platform.webservices.netsuite.com"
               xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:tns="urn:platform_2024_2.webservices.netsuite.com"
               xmlns:tranPurch="urn:purchases_2024_2.transactions.webservices.netsuite.com"
               xmlns:platformCore="urn:core_2024_2.platform.webservices.netsuite.com"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
                <platformMsgs:record xsi:type="tranPurch:VendorPayment">
                    {payload}
                    <tranPurch:applyList>
                        <tranPurch:apply>
                            <tranPurch:doc internalId="{bill_id}"/>
                            <tranPurch:apply>true</tranPurch:apply>
                            <tranPurch:amount>{bill_obj.get("balance")}</tranPurch:amount>
                        </tranPurch:apply>
                    </tranPurch:applyList>
                </platformMsgs:record>
                </platformMsgs:add>
            </soap:Body>
        </soap:Envelope>"""

        headers = {"SOAPAction": "add", "Content-Type": "text/xml"}
        self.logger.info(f"Making request = {base_request}")
        res = requests.post(url, headers=headers, data=base_request)
        self.logger.info(f"Got response = {res.text}")
        if res.status_code >= 400:
            raise ConnectionError(res.text)
        return res

    def push_payments(self, payload):
        url = f"https://{self.url_account}.suitetalk.api.netsuite.com/services/NetSuitePort_2024_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        payload = payload.decode()
        payload = "\n".join(payload.split("\n")[1:-2])

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2024_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
                <platformMsgs:record xsi:type="tranCust:CustomerPayment" xmlns:tranCust="urn:customers_2024_2.transactions.webservices.netsuite.com">
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
        url = f"https://{self.config['ns_account']}.suitetalk.api.netsuite.com/services/NetSuitePort_2024_2"
        oauth_creds = self.ns_client.ns_client._build_soap_headers()
        oauth_creds = oauth_creds["tokenPassport"]

        po_number = payload["poNumber"]

        ns_account = self.config["ns_account"].replace("-", "_").upper()
        oauth = OAuth1(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

        response = self.ns_client.entities["PurchaseOrder"].get_all(
            ["entity", "location"], tran_id=po_number
        )[0]
        po_id = response.get("internalId")

        entity_id = response["entity"]["internalId"]
        location_id = response["location"]["internalId"]

        base_request = f"""<soap:Envelope xmlns:platformFaults="urn:faults_2024_2.platform.webservices.netsuite.com" xmlns:platformMsgs="urn:messages_2024_2.platform.webservices.netsuite.com" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tns="urn:platform_2024_2.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
               <add xmlns="urn:messages_2024_2.platform.webservices.netsuite.com">
                  <record xsi:type="ns6:VendorBill" xmlns:ns6="urn:purchases_2024_2.transactions.webservices.netsuite.com">
                     <ns6:entity internalId="{entity_id}" xsi:type="ns7:RecordRef" xmlns:ns7="urn:core_2024_2.platform.webservices.netsuite.com"/>
                     <ns6:location internalId="{location_id}" xsi:type="ns7:RecordRef" xmlns:ns7="urn:core_2024_2.platform.webservices.netsuite.com"/>
                     <ns6:purchaseOrderList xsi:type="ns8:RecordRefList" xmlns:ns8="urn:core_2024_2.platform.webservices.netsuite.com">
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
        customers = context["reference_data"]["Customer"]
        subsidiary = record.get("subsidiary")
        sales_rep = record.get("ownerId")
        first_name = None
        last_name = None

        if record.get("contactName"):
            names = record.get("contactName").split(" ")
            if len(names) > 0:
                first_name = names[0]
                last_name = " ".join(names[1:])
            else:
                first_name = names[0]
                last_name = ""

        # Handle if the input data is str
        if record.get("addresses") and isinstance(record["addresses"], str):
            record["addresses"] = json.loads(record["addresses"])

        if record.get("phoneNumbers") and isinstance(record["phoneNumbers"], str):
            record["phoneNumbers"] = json.loads(record["phoneNumbers"])

        address_book = [
            {
                "addressBookAddress": {
                    "addr1": address.get("line1"),
                    "addr2": address.get("line2"),
                    "addr3": address.get("line3"),
                    "city": address.get("city"),
                    "state": address.get("state"),
                    "zip": address.get("postalCode"),
                    "country": {"refName": address.get("country").strip()},
                }
            }
            for address in record.get("addresses", [])
        ]

        address = record.get("addresses")
        customer = {
            "companyName": record.get("customerName"),
            "email": record.get("emailAddress"),
            "phone": record.get("phoneNumbers")[0].get("number")
            if record.get("phoneNumbers")
            else None,
            "comments": record.get("notes"),
            "balance": record.get("balance"),
            "dateCreated": record.get("createdAt"),
            "isInactive": not record.get("active", True),
            "addressBook": {"items": address_book},
            "defaultAddress": f"{address[0].get('line1')} {address[0].get('line2', '')} {address[0].get('line3', '')}, {address[0].get('city', '')} {address[0].get('postalCode', '')}, {address[0].get('state', ''), address[0].get('country', '')}"
            if address
            else None,
            "externalId": record.get("id"),
        }

        # If this companyName already exists, we should do a PATCH instead, just need to set id
        existing_customer = [c for c in customers if c.get("externalId") == customer.get("externalId")]
        if not existing_customer:
            existing_customer = [ c for c in customers if c.get("companyName") == customer.get("companyName")]
        if existing_customer:
            customer["id"] = existing_customer[0]["internalId"]

        if first_name:
            customer["firstName"] = first_name
            customer["lastName"] = last_name

        if subsidiary:
            customer["subsidiary"] = {"id": subsidiary}
        if sales_rep:
            customer["salesRep"] = {"id": sales_rep}
        if record.get("currency"):
            customer["currency"] = {"refName": record["currency"]}

        if record.get("customFields"):
            if isinstance(record["customFields"], str):
                record["customFields"] = json.loads(record["customFields"])

            for field in record.get("customFields"):
                if field.get("name"):
                    customer[field["name"]] = field["value"]
                else:
                    self.logger.info(f"Skipping custom field {field} because name is empty")

        return customer

    def process_credit_memo(self, context, record):
        
        if not record.get("customerName") and not record.get("customerId"):
            raise Exception(f"Neither CustomerId nor customerName was provided and it's a required field for credit memo.")
        
        # validate required field customer
        customer = record.get("customerId")
        if not customer and record.get("customerName"):
            customer = list(
                filter(
                    lambda x: x["companyName"] == record.get("customerName"),
                    context["reference_data"]["Customer"],
                )
            )
            customer = customer[0].get("internalId") if customer else None
        if not customer:
            raise Exception(f"Customer '{record.get('customerName')}' was not found in netsuite and it's a required field for credit memo.")

        # validate required field location
        location = record.get("locationId")
        if not location and record.get("location"):
            location = list(
                filter(
                    lambda x: x["name"] == record.get("location"),
                    context["reference_data"]["Locations"],
                )
            ) 
            location = location[0].get("internalId") if location else None
        
        # validate required field location
        subsidiary = record.get("subsidiaryId")
        if not subsidiary and record.get("subsidiary"):
            subsidiary = list(
                filter(
                    lambda x: x["name"] == record.get("subsidiary"),
                    context["reference_data"]["Subsidiaries"],
                )
            ) 
            subsidiary = subsidiary[0].get("internalId") if subsidiary else None

        line_items = self.parse_objs(record.get("lineItems"))
        items = []
        for item in line_items:
            if not item.get("productId") and not item.get("productName"):
                raise Exception(f"Neither ProductId nor productName was provided and it's a required field for credit memo line.")
            
            product = item.get("productId")
            if not product and item.get("productName"):
                product = list(
                    filter(
                        lambda x: x["itemId"] == item["productName"],
                        context["reference_data"]["Items"],
                    )
                )
                product = product[0].get("internalId") if product else None
            if not product:
                raise Exception(f"Item with name '{item.get('productName')}' was not found in netsuite  and it's a required field for credit memo line.")

            item_mapping = {
                "amount": item.get("totalAmount"),
                "quantity": item.get("quantity"),
                "item": {"id": product}
            }
            items.append(item_mapping)

    
        credit_memo_mapping = {
            "id": record.get("id"),
            "memo": record.get("note"),
            "status": {"id": record.get("status")},
            "subtotal": record.get("subtotal"),
            "tranDate": record.get("issueDate"),
            "entity": {"id": customer},
            "total": record.get("total"),
            "discountTotal": record.get("totalDiscount"),
            "taxTotal": record.get("totalTaxAmount"),
            "amountRemaining": record.get("remainingCredit"),
            "item": {"items": items},
            "location": {"id": location},
            "subsidiary": {"id": subsidiary}
        }

        # if transactionNumber is provided, we should use it as the tranId
        if record.get("transactionNumber"):
            credit_memo_mapping["tranId"] = record["transactionNumber"]
        
        # currency is the symbol, we need to send the name
        currency = record.get("currency")
        if currency:
            currency = list(
                filter(
                    lambda x: x["symbol"] == currency,
                    context["reference_data"]["Currencies"],
                )
            )
            currency = currency[0].get("name") if currency else None
            if currency:
                credit_memo_mapping["currency"] = {"refName": currency}

        return credit_memo_mapping
    
    def process_refund(self, context, record):
        
        # validate required field currency
        if not record.get("currency"):
            raise Exception(f"Currency was not provided and it's a required field for credit memo.")
        
        # validate required field customer
        customer = record.get("customerId")
        if not customer and record.get("customerName"):
            customer = list(
                filter(
                    lambda x: x["companyName"] == record.get("customerName"),
                    context["reference_data"]["Customer"],
                )
            )
            customer = customer[0].get("internalId") if customer else None
        if not customer:
            raise Exception(f"Customer '{record.get('customerName')}' with id '{record.get('customerId')}' was not provided or it's not valid and it's a required field for refund.")

        # validate required field location
        location = record.get("locationId")
        if not location and record.get("location"):
            location = list(
                filter(
                    lambda x: x["name"] == record.get("location"),
                    context["reference_data"]["Locations"],
                )
            ) 
            location = location[0].get("internalId") if location else None
        
        # validate subsidiary
        subsidiary = record.get("subsidiaryId")
        if not subsidiary and record.get("subsidiary"):
            subsidiary = list(
                filter(
                    lambda x: x["name"] == record.get("subsidiary"),
                    context["reference_data"]["Subsidiaries"],
                )
            ) 
            subsidiary = subsidiary[0].get("internalId") if subsidiary else None

        line_items = self.parse_objs(record.get("lineItems"))
        items = []
        for item in line_items:
            product = item.get("productId")
            if not product and item.get("productName"):
                product = list(
                    filter(
                        lambda x: x["itemId"] == item["productName"],
                        context["reference_data"]["Items"],
                    )
                )
                product = product[0].get("internalId") if product else None
            if not product:
                raise Exception(f"Item with name '{record.get('productName')}' and id '{record.get('productId')} was not found for credit memo line.'")

            item_mapping = {
                "amount": item.get("totalAmount"),
                "quantity": item.get("quantity"),
                "item": {"id": product}
            }
            items.append(item_mapping)

    
        refund_mapping = {
            "id": record.get("id"),
            "memo": record.get("note"),
            "status": {"id": record.get("status")},
            "currency": {"refName": record.get("currency")},
            "subtotal": record.get("subtotal"),
            "tranDate": record.get("issueDate"),
            "entity": {"id": customer},
            "total": record.get("total"),
            "discountTotal": record.get("totalDiscount"),
            "taxTotal": record.get("totalTaxAmount"),
            "amountRemaining": record.get("remainingCredit"),
            "item": {"items": items},
            "location": {"id": location},
            "subsidiary": {"id": subsidiary}
        }

        return refund_mapping

    def process_vendors(self, context, record):
        vendors = context["reference_data"]["Vendors"]
        vendor = None
        if record.get("id"):
            vendor = list(
                filter(
                    lambda x: x["internalId"] == record.get("id")
                    or x["externalId"] == record.get("id"),
                    vendors,
                )
            )

        address = record.get("addresses")
        phoneNumber = record.get("phoneNumbers")
        vendor_mapping = {
            "email": record.get("emailAddress"),
            "companyName": record.get("vendorName"),
            "dateCreated": record.get("createdAt"),
            "entityId": record.get("vendorName"),
            "firstName": record.get("contactName"),
            "lastModifiedDate": record.get("updatedAt"),
            "currency": {"refName": record.get("currency")},
            "homePhone": phoneNumber[0]["number"] if phoneNumber else None,
            "defaultAddress": f"{address[0]['line1']} {address[0]['line2']} {address[0]['line3']}, {address[0]['city']}, {address[0]['state'], address[0]['country'], address[0]['postalCode']}"
            if address
            else None,
        }

        if record.get("subsidiary"):
            vendor_mapping["subsidiary"] = {"id": record.get("subsidiary")}

        if vendor:
            vendor_mapping["internalId"] = vendor[0].get("internalId")
            vendor_mapping["accountNumber"] = vendor[0].get("accountNumber")

        return vendor_mapping
    
    def process_service_sale_item(self, context, record):

        payload = {
            "displayName": record.get("displayName") or record.get("name"),
            "itemId": record.get("name"),
            "upcCode": record.get("code"),
            "createdAt": record.get("createdAt"),
            "isInactive": not record.get("active", True),
            "itemType": {"refName": "Service"},
            "type": "service for sale" # value not sent to netsuite, only used locally 
        }
        
        subsidiary = record.get("subsidiary", record.get("subsidiaryId"))
        if context["reference_data"].get("Subsidiaries"):
            subsidiary_obj = [
                sub
                for sub in context["reference_data"].get("Subsidiaries")
                if sub.get("name") == subsidiary or sub.get("internalId") == subsidiary
            ]
            if subsidiary_obj:
                payload["subsidiary"] = {
                    "items": [{"id": subsidiary_obj[0]["internalId"]}]
                }

        # for incomeAccount field find a match by id or name if the right payload ({"id": "sale account id"}) is not passed
        accounts = self.parse_objs(record.get("accounts", "{}"))
        if accounts.get("income"):
            income_account = accounts.get("income")

            account = list(
                filter(
                    lambda x: self.get_account_by_name_id_number(
                        x,
                        income_account.get("accountName"),
                        income_account.get("accountId"),
                        income_account.get("accountNumber"),
                    ),
                    context["reference_data"]["Accounts"],
                )
            )
            if account:
                payload["incomeAccount"] = {"id": account[0]["internalId"]}

        if not payload.get("incomeAccount"):
            raise Exception(
                f"Service sale item can't be created without an incomeAccount, please provide an incomeAccount"
            )

        custom_fields = self.parse_objs(record.get("customFields", "[]"))
        for cf in custom_fields:
            # add custom fields to payload
            if cf.get("name"):
                payload[cf.get("name")] = cf.get("value")

        return payload

    def process_item(self, context, record):

        if record.get("type", "").lower() == "service for sale":
            return self.process_service_sale_item(context, record)

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
                    lambda x: self.get_account_by_name_id_number(x, accountName, id),
                    context["reference_data"]["Accounts"],
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
                        lambda x: self.get_account_by_name_id_number(x, accountName, id),
                        context["reference_data"]["Accounts"],
                    )
                )[0]
                payload["incomeAccount"] = {"id": account["internalId"]}

        return payload

    def process_purchase_order(self, context, record):
        purchase_order = {}
        if record.get("order_number"):
            purchase_order["externalId"] = record["order_number"]
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
        elif context["reference_data"].get("Vendors") and record.get("vendorName"):
            vendor_names = []
            for c in context["reference_data"]["Vendors"]:
                if "entityId" in c.keys():
                    vendor_names.append(c["entityId"])
            vendor_name = self.get_close_matches(
                record["vendorName"], vendor_names, n=2, cutoff=0.9
            )
            if vendor_name:
                vendor_name = max(vendor_name, key=vendor_name.get)
                vendor_data = []
                for c in context["reference_data"]["Vendors"]:
                    if c["entityId"] == vendor_name:
                        vendor_data.append(c)
                if vendor_data:
                    vendor_data = vendor_data[0]
                    purchase_order["entity"] = {"id": vendor_data.get("internalId")}
        # Prevent parse function from failing on empty date
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
        elif context["reference_data"].get("Locations") and record.get("location"):
            loc_data = [
                l
                for l in context["reference_data"]["Locations"]
                if l["name"] == record["location"]
            ]
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
            elif context["reference_data"].get("Items") and line.get("product_name"):
                product_names = [
                    c["itemId"] for c in context["reference_data"]["Items"]
                ]
                product_name = self.get_close_matches(
                    line["product_name"], product_names, n=2, cutoff=0.95
                )
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [
                        c
                        for c in context["reference_data"]["Items"]
                        if c["itemId"] == product_name
                    ]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}
            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = round(
                line.get("quantity") * line.get("unit_price"), 3
            )

            items.append(order_item)
        if items:
            purchase_order["item"] = {"items": items}

        return purchase_order
