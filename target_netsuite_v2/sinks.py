"""netsuite-v2 target sink class, which handles writing streams."""

from typing import Any


from singer_sdk.plugin_base import PluginBase
from lxml import etree
from target_netsuite_v2.soap_client import netsuiteSoapV2Sink
from target_netsuite_v2.rest_client import netsuiteRestV2Sink
from target_netsuite_v2.zeep_soap_client import NetsuiteSoapClient


class netsuiteV2Sink(netsuiteSoapV2Sink, netsuiteRestV2Sink):
    """netsuite-v2 target sink class."""


    @property
    def name(self) -> str:
        return self.stream_name

    allows_externalid = [
        "JournalEntries",
        "journalentries",
        "JournalEntry",
        "journalentry",
        "CustomerPayment",
        "customerpayment",
        "CustomerPayments",
        "customerpayments",
        "VendorBill",
        "vendorbill",
        "VendorBills",
        "vendorbills",
        "PurchaseInvoice",
        "purchaseinvoices",
        "PurchaseInvoices",
        "purchaseinvoice",
        "Bill",
        "bill",
        "Bills",
        "bills",
    ]

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema,
        key_properties,
    ) -> None:
        self._state = dict(target._state)
        self._target = target

        super().__init__(target, stream_name, schema, key_properties)
        
        self.get_ns_client()
        self.reference_data = self.get_reference_data()

    def _context_with_reference_data(self, context):
        context["reference_data"] = self.reference_data
        return context

    def _extract_id_from_soap_response(self, response):
        content = getattr(response, "content", None) or getattr(response, "text", "")
        if not content:
            return None
        if isinstance(content, str):
            content = content.encode()
        root = etree.fromstring(content)
        for element in root.iter():
            if element.tag.endswith("baseRef"):
                return element.get("internalId")
        return None

    
    def post_item(self, record):
        ns = NetsuiteSoapClient(self.config)
        service = ns.service_proxy
        soap_headers = ns.build_headers()
        response = service.add(record, _soapheaders=soap_headers)
        try:
            is_duplicated = response['body']['writeResponse']['status']['statusDetail'][0]['code'] == 'DUP_ITEM'
        except:
            is_duplicated = False
        if not response['body']['writeResponse']['status']['isSuccess'] and not is_duplicated:
            raise Exception(response['body']['writeResponse']['status']['statusDetail'][0]['message'])
        elif is_duplicated:
            self.logger.info(f"This item has already been posted: {record.itemId}")
        else:
            self.logger.info(f"Item with itemId {record.itemId} posted successfully")

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        context = self._context_with_reference_data(context)
        if self.stream_name.lower() in ["journalentries", "journalentry"]:
            journal_entry = self.process_journal_entry(context, record)
            return journal_entry
        if self.stream_name.lower() in ["customer", "customers"]:
            customer = self.process_customer(context,record)
            return customer
        if self.stream_name.lower() in ["inboundshipment","inboundshipments"]:
            inbound_shipment = self.process_inbound_shipment(context, record)
            return inbound_shipment
        elif self.stream_name.lower() in ["customerpayment","customerpayments"]:
            customer_payment = self.process_customer_payment(context, record)
            return customer_payment
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            invoice = self.process_invoice(context, record)
            return invoice
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            credit_memo = self.process_credit_memo(context, record)
            return credit_memo
        elif self.stream_name.lower() in ["vendor","vendors"]:
            vendor = self.process_vendors(context, record)
            return vendor
        elif self.stream_name.lower() in ["vendorbill", "vendorbills", "purchaseinvoices","purchaseinvoice", "bill", "bills"]:
            vendor_bill = self.process_vendor_bill(context, record)
            return vendor_bill
        elif self.stream_name.lower() in ["invoicepayments","invoicepayment"]:
            return record
        elif self.stream_name.lower() in ["vendorpayments","vendorpayment","billpayments","billpayment"]:
            # vendor_payment = self.vendor_payment(context, record)
            return record
        elif self.stream_name.lower() in ["PurchaseOrderToVendorBill"]:
            return record
        elif self.stream_name.lower() in ['item','items']:
            item = self.process_item(context,record)
            return item
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            order = self.process_purchase_order(context,record)
            return order
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            sale_order = self.process_order(context, record)
            return sale_order

        raise Exception(f"Stream {self.stream_name} not supported")

    def get_record_url(self, record_id):
        entity_mapping = {
            "journalentries": "accounting/transactions/journal",
            "journalentry": "accounting/transactions/journal",
            "customer": "common/entity/custjob",
            "customers": "common/entity/custjob",
            "customerpayment": "accounting/transactions/custpymt",
            "customerpayments": "accounting/transactions/custpymt",
            "invoice": "accounting/transactions/custinvc",
            "invoices": "accounting/transactions/custinvc",
            "creditmemo": "accounting/transactions/custcred",
            "creditmemos": "accounting/transactions/custcred",
            "vendor": "common/entity/vendor",
            "vendors": "common/entity/vendor",
            "vendorbill": "accounting/transactions/vendbill",
            "vendorbills": "accounting/transactions/vendbill",
            "purchaseinvoice": "accounting/transactions/vendbill",
            "purchaseinvoices": "accounting/transactions/vendbill",
            "bill": "accounting/transactions/vendbill",
            "bills": "accounting/transactions/vendbill",
            "invoicepayments": "accounting/transactions/custpymt",
            "invoicepayment": "accounting/transactions/custpymt",
            "vendorpayments": "accounting/transactions/vendpymt",
            "vendorpayment": "accounting/transactions/vendpymt",
            "billpayments": "accounting/transactions/vendpymt",
            "billpayment": "accounting/transactions/vendpymt",
            "item": "common/entity/item",
            "items": "common/entity/item",
            "purchaseorder": "inventory/transactions/inboundShipment",
            "purchaseorders": "inventory/transactions/inboundShipment",
            "salesorder": "inventory/transactions/inboundShipment",
            "salesorders": "inventory/transactions/inboundShipment",
        }
        entity = entity_mapping.get(self.stream_name.lower())

        # get base url
        if entity:
            base_url = self.get_base_url()
            return f"{base_url}/app/{entity}.nl?id={record_id}"

    def upsert_record(self, record, context):
        """Write out any prepped records and return once fully written."""
        self.logger.info(f"Posting data for entity {self.stream_name}")
        response = None
        name = None
        record_id = None
        context = self._context_with_reference_data(context)

        if self.stream_name.lower() in ["journalentries", "journalentry", "customerpayment"]:
            if self.stream_name.lower() in ["journalentries", "journalentry"]:
                name = "JournalEntry"
            else:
                name = "CustomerPayment"
            
            response = self.ns_client.entities[name].post(record)
            self.logger.info(response)
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            url = f"{self.url_base}salesOrder"
            
            if record.get("id") is None:
                response = self.rest_post(url=url, json=record)
            else:
                self.logger.info(f"Updating Order: {record.get('id')}")
                response = self.rest_patch(url=f"{url}/{record.pop('id')}", json=record)
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            url = f"{self.url_base}invoice"
            if record.get("tranId"):
                existing = self.rest_get(url=f"{url}?q=tranid IS {record['tranId']}").json()
                if existing.get("count") > 0:
                    record_id = existing["items"][0]["id"]
                    if "item" in record:
                        del record["item"]
                    if "currency" in record:
                        del record["currency"]
                    response = self.rest_patch(url=f"{url}/{record_id}", json=record)
                    state_update = {"existing": True}
                    if self.config.get("output_record_url", False):
                        state_update["record_url"] = self.get_record_url(record_id)
                    return record_id, True, state_update
            response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:   
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendor","vendors"]:   
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendorbill","vendorbills","bill","bills","purchaseinvoices","purchaseinvoice"]:
            url = f"{self.url_base}vendorbill"

            attachments = record.pop("attachments", [])
            attachment_ids = self.process_file(attachments, record)

            response = self.rest_post(url=url, json=record)
            new_record_id = self._extract_id_from_response_header(response.headers)

            for attachment_id in attachment_ids:
                self.attach_entities(attachment_id, "vendorBill", new_record_id)
        elif self.stream_name.lower() in ["invoicepayment","invoicepayments"]:
            payload = self.invoice_payment(context, record)
            response = self.push_payments(payload)
        elif self.stream_name.lower() in ["vendorpayment","vendorpayments","billpayment","billpayments"]:
            payload = self.vendor_payment(context, record, self.reference_data)
            response = self.push_vendor_payments(payload)
        elif self.stream_name in ["PurchaseOrderToVendorBill"]:
            response = self.po_to_vb(record)
        elif self.stream_name.lower() in ['inboundshipment','inboundshipments']:
            if record.get("id"):
                endpoint="inboundShipment"
                endpoint = endpoint + "/{id}"
                endpoint = endpoint.format(id=record.pop("id"))
                url = f"{self.url_base}{endpoint}"
                response = self.rest_patch(url=url, json=record)
            else: 
                response = self.ns_client.entities["InboundShipment"].post(record)
                
            self.logger.info(response)
        elif self.stream_name.lower() in ['customer','customers']:
            url = f"{self.url_base}customer"
            customer_subsidiary_relationships = record.pop("customerSubsidiaryRelationships", None)
            record_id = record.pop("id", None)
            if record_id:
                response = self.rest_patch(url=f"{url}/{record_id}", json=record)
                self.logger.info(f"Customer with id '{record_id}' updated")
            else:
                response = self.rest_post(url=url, json=record)
                record_id = self._extract_id_from_response_header(response.headers)
                self.logger.info(f"Customer with id '{record_id}' created")

            if customer_subsidiary_relationships:
                relationship_url = f"{self.url_base}customerSubsidiaryRelationship"
                existing_relationship_objects = self.get_customer_subsidiary_relationships(record_id)
                subsidiaries_already_linked = [
                    relationship.get("subsidiary")
                    for relationship in existing_relationship_objects
                ]
                for relationship in customer_subsidiary_relationships:
                    if relationship.get("subsidiary", {}).get("id") in subsidiaries_already_linked:
                        continue
                    self.logger.info(
                        f"Creating customer subsidiary relationship for customer {record_id} and subsidiary {relationship.get('subsidiary')}"
                    )
                    relationship["entity"] = {"id": record_id}
                    relationship_response = self.rest_post(url=relationship_url, json=relationship)
                    self.logger.info(relationship_response)
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            url = f"{self.url_base}purchaseOrder"

            attachments = record.pop("attachments", [])
            attachment_ids = self.process_file(attachments, record)

            response = self.rest_post(url=url, json=record)
            new_record_id = self._extract_id_from_response_header(response.headers)

            for attachment_id in attachment_ids:
                self.attach_entities(attachment_id, "purchaseOrder", new_record_id)

        state_update = {}
        if response:
            if name in ["JournalEntry", "CustomerPayment"]:
                try:    
                    record_id = response["internalId"]
                except:
                    raise Exception(f"Internal ID not found for {name}, response: {response}")
            elif self.stream_name.lower() in ["invoicepayment","invoicepayments"]:
                record_id = self._extract_id_from_soap_response(response)
            else:
                record_id = record_id or self._extract_id_from_response_header(response.headers)
            if self.config.get("output_record_url", False):
                record_url = self.get_record_url(record_id)
                state_update["record_url"] = record_url
            return record_id, True, state_update
        else:
            return None, True, state_update
