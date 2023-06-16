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
        context["VendorPayment"] = []
        context["Vendor"] = []
        context["PurchaseOrderToVendorBill"] = []
        context["InboundShipment"] = []
        context['CreditMemo'] = []
        context['Customer'] = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if self.stream_name.lower() in ["journalentries", "journalentry"]:
            journal_entry = self.process_journal_entry(context, record)
            context["JournalEntry"].append(journal_entry)
        
        if self.stream_name.lower() in ["customer"]:
            customer = self.process_customer(context,record)
            context['Customer'].append(customer)
        if self.stream_name=="InboundShipment":
            inbound_shipment = self.process_inbound_shipment(context, record)
            context["InboundShipment"].append(inbound_shipment)
        elif self.stream_name=="CustomerPayment":
            customer_payment = self.process_customer_payment(context, record)
            context["CustomerPayment"].append(customer_payment)
        elif self.stream_name=="SalesOrder":
            sale_order = self.process_order(context, record)
            context["SalesOrder"].append(sale_order)
        elif self.stream_name=="Invoice":
            invoice = self.process_invoice(context, record)
            context["Invoice"].append(invoice)
        elif self.stream_name == "CreditMemo":
            credit_memo = self.process_credit_memo(context, record)
            context["CreditMemo"].append(credit_memo)
        elif self.stream_name == "Vendor":
            vendor = self.process_vendors(context, record)
            context["Vendor"].append(vendor)
        elif self.stream_name in ["vendorBill", "VendorBill", "PurchaseInvoices", "Bill", "Bills", "bill", "bills"]:
            vendor_bill = self.process_vendor_bill(context, record)
            context["vendorBill"].append(vendor_bill)
        elif self.stream_name=="InvoicePayment":
            invoice_payment = self.invoice_payment(context, record)
            context["InvoicePayment"].append(invoice_payment)
        elif self.stream_name=="VendorPayment":
            vendor_payment = self.vendor_payment(context, record)
            context["VendorPayment"].append(vendor_payment)
        elif self.stream_name=="PurchaseOrderToVendorBill":
            context["PurchaseOrderToVendorBill"].append(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        self.logger.info(f"Posting data for entity {self.stream_name}")
        if self.stream_name.lower() in ["journalentries", "journalentry", "customerpayment", "inboundshipment"]:
            if self.stream_name.lower() in ["journalentries", "journalentry"]:
                name = "JournalEntry"
            else:
                name = self.stream_name
            for record in context.get(name, []):
                response = self.ns_client.entities[name].post(record)
                self.logger.info(response)
        elif self.stream_name in ["SalesOrder"]:
            url = f"{self.url_base}salesOrder"
            for record in context.get(self.stream_name, []):
                if record.get("order_number") is None:
                    response = self.rest_post(url=url, json=record)
                else:
                    self.logger.info(f"Updating Order: {record.get('order_number')}")
                    response = self.rest_patch(url=f"{url}/{record.pop('order_number')}", json=record)
        elif self.stream_name in ["Invoice"]:
            endpoint = list(self.stream_name)
            endpoint[0] = endpoint[0].lower()
            endpoint = "".join(endpoint)
            url = f"{self.url_base}{endpoint}"
            for record in context.get(self.stream_name, []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name in ["CreditMemo"]:   
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            for record in context.get(self.stream_name, []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name in ["Vendor"]:   
            endpoint = self.stream_name.lower()
            url = f"{self.url_base}{endpoint}"
            for record in context.get(self.stream_name, []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name in ["vendorBill", "VendorBill", "PurchaseInvoices", "Bill", "Bills", "bill", "bills"]:
            url = f"{self.url_base}vendorbill"
            for record in context.get("vendorBill", []):
                response = self.rest_post(url=url, json=record)
        elif self.stream_name in ["InvoicePayment"]:
            for record in context.get(self.stream_name, []):
                response = self.push_payments(record)
        elif self.stream_name in ["VendorPayment"]:
            for record in context.get(self.stream_name, []):
                response = self.push_vendor_payments(record)
        elif self.stream_name in ["PurchaseOrderToVendorBill"]:
            for record in context.get(self.stream_name, []):
                response = self.po_to_vb(record)
        elif self.stream_name in ["InboundShipment"]:
            endpoint = list(self.stream_name)
            endpoint[0] = endpoint[0].lower()
            endpoint = "".join(endpoint)
            endpoint = endpoint + "/{id}"
            for record in context.get(self.stream_name, []):
                endpoint = endpoint.format(id=record.pop("id"))
                url = f"{self.url_base}{endpoint}"
                response = self.rest_patch(url=url, json=record)
        elif self.stream_name in ['Customer']:
            url = f"{self.url_base}{self.stream_name.lower()}"
            for record in context.get(self.stream_name, []):
                response = self.rest_post(url=url, json=record)




