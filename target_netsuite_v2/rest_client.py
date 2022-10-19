"""netsuite-v2 target sink class, which handles writing streams."""

from singer_sdk.sinks import BatchSink
import requests
from oauthlib import oauth1
from requests_oauthlib import OAuth1
from pendulum import parse
import json
from lxml import etree


class netsuiteRestV2Sink(BatchSink):
    """netsuite-v2 target sink class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/record/v1/"

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
        if response.status_code>=400:
            try:
                self.logger.error(json.dumps(response.json().get("o:errorDetails")))
                self.logger.error(f"INVALID PAYLOAD: {json.dumps(kwarg['json'])}")
                response.raise_for_status()
            except:
                response.raise_for_status()
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
        if response.status_code>=400:
            try:
                self.logger.error(json.dumps(response.json().get("o:errorDetails")))
                self.logger.error(f"INVALID PAYLOAD: {json.dumps(kwarg['json'])}")
                response.raise_for_status()
            except:
                response.raise_for_status()
        return response

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
            customer_name = self.get_close_matches(record["customer_name"], customer_names, n=2, cutoff=0.95)
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
                product_names = [c["itemId"] for c in context["reference_data"]["Items"]]
                product_name = self.get_close_matches(line["product_name"], product_names, n=2, cutoff=0.95)
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [c for c in context["reference_data"]["Items"] if c["itemId"]==product_name]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("unit_price")
            items.append(order_item)
        sale_order["item"] = {"items": items}
        # Get order number
        if record.get("order_number") is not None:
            sale_order['order_number'] = record.get("order_number")
        return sale_order


    def process_invoice(self, context, record):
        invoice = {}
        items = []

        # Get the NetSuite Customer Ref
        if context["reference_data"].get("Customer") and record.get("customerName"):
            customer_names = []
            for c in context["reference_data"]["Customer"]:
                if "name" in c.keys():
                    if c["name"]:
                        customer_names.append(c["name"])
                else:
                    if c["companyName"]:
                        customer_names.append(c["companyName"])
            customer_name = self.get_close_matches(record["customerName"], customer_names, n=2, cutoff=0.5)
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
            loc_data = [l for l in context["reference_data"]["Locations"] if l["name"] == record["location"]]
            if loc_data:
                loc_data = loc_data[0]
                location = {"id": loc_data.get("internalId")}
        else:
            location = {"id": record.get("locationId", "1")}
        
        invoice["Location"] = location

        asofdate = record.get("dueDate")
        if isinstance(asofdate, str):
            asofdate = parse(asofdate)
        invoice["asofdate"] = asofdate.strftime("%Y-%m-%d")
        
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
            if context["reference_data"].get("Items") and line.get("productName"):
                product_names = [c["itemId"] for c in context["reference_data"]["Items"]]
                product_name = self.get_close_matches(line["productName"], product_names, n=2, cutoff=0.95)
                if product_name:
                    product_name = max(product_name, key=product_name.get)
                    product_data = [c for c in context["reference_data"]["Items"] if c["itemId"]==product_name]
                    if product_data:
                        product_data = product_data[0]
                        order_item["item"] = {"id": product_data.get("internalId")}

            order_item["quantity"] = line.get("quantity")
            order_item["amount"] = line.get("unitPrice")
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

        headers = {"SOAPAction":"initialize", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code>=400:
            raise ConnectionError(res.text)
        res_xml = etree.fromstring(res.text.encode())
        record = res_xml[1][0][0][-1]

        for r in record:
            if isinstance(r.text, str):
                r.getparent().remove(r)

        return etree.tostring(record, pretty_print=True)

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
    
        headers = {"SOAPAction":"add", "Content-Type": "text/xml"}
        res = requests.post(url, headers=headers, data=base_request)
        if res.status_code>=400:
            raise ConnectionError(res.text)
        return res
