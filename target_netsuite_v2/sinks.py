"""netsuite-v2 target sink class, which handles writing streams."""

from target_netsuite_v2.soap_client import netsuiteSoapV2Sink
from target_netsuite_v2.rest_client import netsuiteRestV2Sink


class netsuiteV2Sink(netsuiteSoapV2Sink, netsuiteRestV2Sink):
    """netsuite-v2 target sink class."""

    def start_batch(self, context: dict) -> None:
        """Start a batch."""

        self.get_ns_client()
        context["reference_data"] = self.get_reference_data()
        context["JournalEntry"] = []
        context["SalesOrder"] = []
        context["Invoice"] = []
        context["InvoicePayment"] = []
        context["vendorBill"] = []
        context["vendorCredit"] = []
        context["VendorPayment"] = []
        context["Vendor"] = []
        context["PurchaseOrderToVendorBill"] = []
        context["InboundShipment"] = []
        context['CreditMemo'] = []
        context['Customer'] = []
        context['Items'] = []
        context['PurchaseOrder'] = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if not record:
            self.logger.info(f"Record is empty for {self.stream_name}")
            return
        if self.stream_name.lower() in ["journalentries", "journalentry"]:
            journal_entry = self.process_journal_entry(context, record)
            context["JournalEntry"].append(journal_entry)
        if self.stream_name.lower() in ["customer"]:
            customer = self.process_customer(context,record)
            context["Customer"].append(customer)
        if self.stream_name.lower() in ["inboundshipment","inboundshipments"]:
            inbound_shipment = self.process_inbound_shipment(context, record)
            context["InboundShipment"].append(inbound_shipment)
        elif self.stream_name.lower() in ["customerpayment","customerpayments"]:
            customer_payment = self.process_customer_payment(context, record)
            context["CustomerPayment"].append(customer_payment)
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            sale_order = self.process_order(context, record)
            context["SalesOrder"].append(sale_order)
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            invoice = self.process_invoice(context, record)
            context["Invoice"].append(invoice)
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            credit_memo = self.process_credit_memo(context, record)
            context["CreditMemo"].append(credit_memo)
        elif self.stream_name.lower() in ["vendor","vendors"]:
            vendor = self.process_vendors(context, record)
            context["Vendor"].append(vendor)
        elif self.stream_name.lower() in ["vendorbill", "vendorbills", "purchaseinvoices","purchaseinvoice", "bill", "bills"]:
            vendor_bill = self.process_vendor_bill(context, record)
            context["vendorBill"].append(vendor_bill)
        elif self.stream_name.lower() in ["vendorcredit", "vendorcredits", "apadjustment", "apadjustments"]:
            vendor_credit = self.process_vendor_credit(context, record)
            context["vendorCredit"].append(vendor_credit)
        elif self.stream_name.lower() in ["invoicepayments","invoicepayment"]:
            invoice_payment = self.invoice_payment(context, record)
            context["InvoicePayment"].append(invoice_payment)
        elif self.stream_name.lower() in ["vendorpayments","vendorpayment"]:
            vendor_payment = self.vendor_payment(context, record)
            context["VendorPayment"].append(vendor_payment)
        elif self.stream_name.lower() in ["PurchaseOrderToVendorBill"]:
            context["PurchaseOrderToVendorBill"].append(record)
        elif self.stream_name.lower() in ['item','items']:
            item = self.process_item(context,record)
            context["Items"].append(item)
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            order = self.process_purchase_order(context,record)
            context['PurchaseOrder'].append(order)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        self.logger.info(f"Posting data for entity {self.stream_name}")
        if self.stream_name.lower() in ["journalentries", "journalentry", "customerpayment"]:
            if self.stream_name.lower() in ["journalentries", "journalentry"]:
                name = "JournalEntry"
            else:
                name = "CustomerPayment"
            for record in context.get(name, []):
                response = self.ns_client.entities[name].post(record)
                self.logger.info(response)
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            url = f"{self.url_base}salesOrder"
            for record in context.get("SalesOrder", []):
                if record.get("order_number") is None:
                    response = self.rest_post(url=url, json=record)
                else:
                    self.logger.info(f"Updating Order: {record.get('order_number')}")
                    response = self.rest_patch(url=f"{url}/{record.pop('order_number')}", json=record)
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            url = f"{self.url_base}invoice"
            for record in context.get("Invoice", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            for record in context.get("CreditMemo", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendor","vendors"]:
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            for record in context.get("Vendor", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendorbill","vendorbills","bill","bills","purchaseinvoices","purchaseinvoice"]:
            url = f"{self.url_base}vendorbill"
            for record in context.get("vendorBill", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendorcredit","vendorcredits","apadjustment","apadjustments"]:
            url = f"{self.url_base}vendorCredit"
            for record in context.get("vendorCredit", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["invoicepayment","invoicepayments"]:
            for record in context.get("InvoicePayment", []):
                response = self.push_payments(record)
        elif self.stream_name.lower() in ["vendorpayment","vendorpayments"]:
            for record in context.get("VendorPayment", []):
                response = self.push_vendor_payments(record)
        elif self.stream_name in ["PurchaseOrderToVendorBill"]:
            for record in context.get("PurchaseOrderToVendorBill", []):
                response = self.po_to_vb(record)
        elif self.stream_name.lower() in ['inboundshipment','inboundshipments']:
            for record in context.get("InboundShipment", []):
                if record.get("id"):
                    endpoint="inboundShipment"
                    endpoint = endpoint + "/{id}"
                    endpoint = endpoint.format(id=record.pop("id"))
                    url = f"{self.url_base}{endpoint}"
                    response = self.rest_patch(url=url, json=record)
                else:
                    response = self.ns_client.entities["InboundShipment"].post(record)

                self.logger.info(response)

        elif self.stream_name.lower() in ['customers','customer']:
            url = f"{self.url_base}{self.stream_name.lower()}"
            for record in context.get("Customer", []):
                if record.get("id"):
                    response = self.rest_patch(url=f"{url}/{record.pop('id')}", json=record)
                else:
                    response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ['item','items']:
            url = f"{self.url_base}inventoryItem"
            for record in context.get("Items",[]):
                response = self.rest_post(url=url,json=record)
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            url = f"{self.url_base}purchaseOrder"
            for record in context.get("PurchaseOrder",[]):
                response = self.rest_post(url=url,json=record)







