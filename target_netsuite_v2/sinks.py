"""netsuite-v2 target sink class, which handles writing streams."""

from target_netsuite_v2.soap_client import netsuiteSoapV2Sink
from target_netsuite_v2.rest_client import netsuiteRestV2Sink


class netsuiteV2Sink(netsuiteSoapV2Sink, netsuiteRestV2Sink):
    """netsuite-v2 target sink class."""

    def start_batch(self, context: dict) -> None:
        """Start a batch."""

        self.get_ns_client()
        context["reference_data"] = self.get_reference_data()
        context["reference_data"]["CustomFields"] = self._fetch_all_custom_fields()
        context["reference_data"]["CustomLists"] = self._fetch_custom_lists()
        context["reference_data"]["CustomRecordTypes"] = self._fetch_custom_record_types()
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
        context['Refund'] = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if not record:
            self.logger.info(f"Record is empty for {self.stream_name}")
            return
        if self.stream_name.lower() in ["journalentries", "journalentry"]:
            journal_entry = self.process_journal_entry(context, record, self.rest_post)
            # do final validation
            for line in journal_entry.get('lineList', []):
                for cf in line.get('customFieldList', []):
                    script_id = cf['scriptId']
                    if not self.check_custom_field(script_id):
                        raise Exception(f"Error parsing custom field, scriptid '{script_id}' is not valid.")

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
        elif self.stream_name.lower() in ["refund","refunds"]:
            refund = self.process_refund(context, record)
            context["Refund"].append(refund)
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
        elif self.stream_name.lower() in ["vendorpayments","vendorpayment", "billpayment", "billpayments"]:
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
                # If there's a tranid, we want to check if the invoice already exists, and if so upsert
                if record.get("tranId"):
                    existing = self.rest_get(url=f"{url}?q=tranid IS {record['tranId']}").json()
                    if existing.get("count") > 0:
                        # we need to use the real netsuite id to do the upsert
                        inv_id = existing["items"][0]["id"]
                        
                        # Since this is an eisting invoice, we delete the item from the invoice.
                        # This is done to work around the where items in netsuite are getting duplicated on update.
                        if 'item' in record:
                            del record['item']
                        # NetSuite does not allow updating currency on existing transactions
                        # see: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/bridgehead_N1398658.html
                        if 'currency' in record:
                            del record['currency']

                        response = self.rest_patch(url=f"{url}/{inv_id}", json=record)
                        continue

                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            url = f"{self.url_base}creditMemo"
            for record in context.get("CreditMemo", []):
                id = record.pop("id", None)
                if id:
                    self.logger.info(f"Updating credit memo: {id}")
                    response = self.rest_patch(url=f"{url}/{id}", json=record)
                else:
                    response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["refund","refunds"]:
            url = f"{self.url_base}cashRefund"
            for record in context.get("Refund", []):
                id = record.pop("id", None)
                if id:
                    self.logger.info(f"Updating refund: {id}")
                    response = self.rest_patch(url=f"{url}/{id}", json=record)
                else:
                    response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendor","vendors"]:
            url = f"{self.url_base}vendor"
            for record in context.get("Vendor", []):
                if record.get("internalId"):
                    response = self.rest_patch(url=f"{url}/{record.pop('internalId')}", json={
                        key: value
                        for key, value in record.items()
                        if value is not None
                    })
                else:
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
        elif self.stream_name.lower() in ["vendorpayment","vendorpayments", "billpayment", "billpayments"]:
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
            subsidiaries = [sub['internalId'] for sub in context["reference_data"].get("Subsidiaries", [])]
            for record in context.get("Customer", []):
                customer_subsidiary_relationships = record.pop("customerSubsidiaryRelationships", None)
                id = record.pop("id", None)
                if id:
                    response = self.rest_patch(url=f"{url}/{id}", json=record)
                    self.logger.info(f"Customer with id '{id}' updated")
                else:
                    response = self.rest_post(url=url, json=record)
                    id = response.headers["Location"].split("/")[-1]
                    self.logger.info(f"Customer with id '{id}' created")
                # add additional subsidiaries to the customer
                if customer_subsidiary_relationships:
                    relationship_url = f"{self.url_base}customerSubsidiaryRelationship"
                    for relationship in customer_subsidiary_relationships:
                        self.logger.info(f"Creating customer subsidiary relationship for customer {id} and subsidiary {relationship.get('subsidiary')}")
                        relationship["entity"] = {"id": id}
                        try:
                            response = self.rest_post(url=relationship_url, json=relationship)
                            self.logger.info(response)
                        except Exception as e:
                            subsidiary_id = relationship.get('subsidiary', {}).get('id')
                            # can't add the same subsidiary to a customer more than once
                            if f"You have entered an Invalid Field Value {subsidiary_id} for the following field: subsidiary" in e.response.text and subsidiary_id in subsidiaries:
                                self.logger.info(f"Customer subsidiary relationship already exists for customer {id} and subsidiary {relationship.get('subsidiary')}")
                            else:
                                raise e


        elif self.stream_name.lower() in ['item','items']:
            url = f"{self.url_base}"
            for record in context.get("Items",[]):
                item_type = record.pop("type", None)
                if item_type == "service for sale":
                    url = f"{self.url_base}serviceSaleItem"
                else:
                    url = f"{self.url_base}inventoryItem"

                if record.get("id"):
                    patch_url = url + "/{id}"
                    patch_url = patch_url.format(id=record.pop("id"))
                    response = self.rest_patch(url=patch_url,json=record)
                else:
                    response = self.rest_post(url=url,json=record)
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            url = f"{self.url_base}purchaseOrder"
            for record in context.get("PurchaseOrder",[]):
                response = self.rest_post(url=url,json=record)
