"""netsuite-v2 target sink class, which handles writing streams."""

from typing import Any


from singer_sdk.plugin_base import PluginBase
from target_netsuite_v2.soap_client import netsuiteSoapV2Sink
from target_netsuite_v2.rest_client import netsuiteRestV2Sink
from target_netsuite_v2.zeep_soap_client import NetsuiteSoapClient


class netsuiteV2Sink(netsuiteSoapV2Sink, netsuiteRestV2Sink):
    """netsuite-v2 target sink class."""


    @property
    def name(self) -> str:
        return self.stream_name

    
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
            vendor_bill["attachment_ids"] = self.process_file(record.get("attachments", []), vendor_bill.get("externalId"))
            return vendor_bill
        elif self.stream_name.lower() in ["invoicepayments","invoicepayment"]:
            invoice_payment = self.invoice_payment(context, record)
            return invoice_payment
        elif self.stream_name.lower() in ["vendorpayments","vendorpayment","billpayments","billpayment"]:
            vendor_payment = self.vendor_payment(context, record)
            return vendor_payment
        elif self.stream_name.lower() in ["PurchaseOrderToVendorBill"]:
            return record
        elif self.stream_name.lower() in ['item','items']:
            item = self.process_item(context,record)
            return item
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            order = self.process_purchase_order(context,record)
            order["attachment_ids"] = self.process_file(record.get("attachments", []), order.get("externalId"))
            return order
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            sale_order = self.process_order(context, record)
            return sale_order

        raise Exception(f"Stream {self.stream_name} not supported")


    def upsert_record(self, record, context):
        """Write out any prepped records and return once fully written."""
        self.logger.info(f"Posting data for entity {self.stream_name}")
        response = None

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

            attachment_ids = record.pop("attachment_ids", [])
            response = self.rest_post(url=url, json=record)
            new_record_id = self._extract_id_from_response_header(response.headers)

            for attachment_id in attachment_ids:
                self.attach_entities(attachment_id, "vendorBill", new_record_id)
        elif self.stream_name.lower() in ["invoicepayment","invoicepayments"]:
            response = self.push_payments(record)
        elif self.stream_name.lower() in ["vendorpayment","vendorpayments","billpayment","billpayments"]:
            response = self.push_vendor_payments(record)
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
            self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            url = f"{self.url_base}purchaseOrder"

            attachment_ids = record.pop("attachment_ids", [])
            response = self.rest_post(url=url, json=record)
            new_record_id = self._extract_id_from_response_header(response.headers)

            for attachment_id in attachment_ids:
                self.attach_entities(attachment_id, "purchaseOrder", new_record_id)

        if response:
            record_id = self._extract_id_from_response_header(response.headers)
            return record_id, True, {}
        else:
            return None, True, {}

