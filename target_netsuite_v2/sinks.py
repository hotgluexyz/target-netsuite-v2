"""netsuite-v2 target sink class, which handles writing streams."""

from singer_sdk.plugin_base import PluginBase
from target_netsuite_v2.soap_client import netsuiteSoapV2Sink
from target_netsuite_v2.rest_client import netsuiteRestV2Sink


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
        self.reference_data["CustomFields"] = self._fetch_all_custom_fields()
        self.reference_data["CustomLists"] = self._fetch_custom_lists()
        self.reference_data["CustomRecordTypes"] = self._fetch_custom_record_types()

    def preprocess_record(self, record: dict, context: dict) -> dict | None:
        """Process the record."""
        if not record:
            self.logger.info(f"Record is empty for {self.stream_name}")
            return
        if self.stream_name.lower() in ["journalentries", "journalentry"]:
            journal_entry = self.process_journal_entry(record, self.rest_post)
            # do final validation
            for line in journal_entry.get('lineList', []):
                for cf in line.get('customFieldList', []):
                    script_id = cf['scriptId']
                    if not self.check_custom_field(script_id):
                        raise Exception(f"Error parsing custom field, scriptid '{script_id}' is not valid.")

            return journal_entry
        if self.stream_name.lower() in ["customer"]:
            customer = self.process_customer(record)
            return customer
        if self.stream_name.lower() in ["inboundshipment","inboundshipments"]:
            inbound_shipment = self.process_inbound_shipment(record)
            return inbound_shipment
        elif self.stream_name.lower() in ["customerpayment","customerpayments"]:
            customer_payment = self.process_customer_payment(record)
            return customer_payment
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            sale_order = self.process_order(record)
            return sale_order
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            invoice = self.process_invoice(record)
            return invoice
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            credit_memo = self.process_credit_memo(record)
            return credit_memo
        elif self.stream_name.lower() in ["refund","refunds"]:
            refund = self.process_refund(record)
            return refund
        elif self.stream_name.lower() in ["vendor","vendors"]:
            vendor = self.process_vendors(record)
            return vendor
        elif self.stream_name.lower() in ["vendorbill", "vendorbills", "purchaseinvoices","purchaseinvoice", "bill", "bills"]:
            vendor_bill = self.process_vendor_bill(record)
            return vendor_bill
        elif self.stream_name.lower() in ["vendorcredit", "vendorcredits", "apadjustment", "apadjustments"]:
            vendor_credit = self.process_vendor_credit(record)
            return vendor_credit
        elif self.stream_name.lower() in ["invoicepayments","invoicepayment"]:
            invoice_payment = self.invoice_payment(record)
            return invoice_payment
        elif self.stream_name.lower() in ["vendorpayments","vendorpayment", "billpayment", "billpayments"]:
            vendor_payment = self.vendor_payment(record)
            return vendor_payment
        elif self.stream_name.lower() in ["PurchaseOrderToVendorBill"]:
            return record
        elif self.stream_name.lower() in ['item','items']:
            item = self.process_item(record)
            return item
        elif self.stream_name.lower() in ['purchaseorder','purchaseorders']:
            order = self.process_purchase_order(record)
            return order

    def upsert_record(self, record, context):
        """Write out any prepped records and return once fully written."""
        self.logger.info(f"Posting data for entity {self.stream_name}")
        name = None
        if self.stream_name.lower() in ["journalentries", "journalentry", "customerpayment", "customerpayments"]:
            if self.stream_name.lower() in ["journalentries", "journalentry"]:
                name = "JournalEntry"
            else:
                name = "CustomerPayment"
            
            response = self.ns_client.entities[name].post(record)
            self.logger.info(response)
        elif self.stream_name.lower() in ["salesorder","salesorders"]:
            url = f"{self.url_base}salesOrder"
            
            if record.get("order_number") is None:
                response = self.rest_post(url=url, json=record)
            else:
                self.logger.info(f"Updating Order: {record.get('order_number')}")
                response = self.rest_patch(url=f"{url}/{record.pop('order_number')}", json=record)
        elif self.stream_name.lower() in ["invoice", "invoices"]:
            url = f"{self.url_base}invoice"
            invoice_already_exists = False
            # If there's a tranid, we want to check if the invoice already exists, and if so upsert
            if record.get("tranId"):
                existing = self.rest_get(url=f"{url}?q=tranid IS {record['tranId']}").json()
                if existing.get("count") > 0:
                    invoice_already_exists = True
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

            if not invoice_already_exists:
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["creditmemo","creditmemos"]:
            url = f"{self.url_base}creditMemo"
            id = record.pop("id", None)
            if id:
                self.logger.info(f"Updating credit memo: {id}")
                response = self.rest_patch(url=f"{url}/{id}", json=record)
            else:
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["refund","refunds"]:
            url = f"{self.url_base}cashRefund"
            
            id = record.pop("id", None)
            if id:
                self.logger.info(f"Updating refund: {id}")
                response = self.rest_patch(url=f"{url}/{id}", json=record)
            else:
                response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendor","vendors"]:
            url = f"{self.url_base}vendor"
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
            response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["vendorcredit","vendorcredits","apadjustment","apadjustments"]:
            url = f"{self.url_base}vendorCredit"
            response = self.rest_post(url=url, json=record)
        elif self.stream_name.lower() in ["invoicepayment","invoicepayments"]:
            response = self.push_payments(record)
        elif self.stream_name.lower() in ["vendorpayment","vendorpayments", "billpayment", "billpayments"]:
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
                name = "InboundShipment"

            self.logger.info(response)

        elif self.stream_name.lower() in ['customers','customer']:
            url = f"{self.url_base}{self.stream_name.lower()}"

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
                existing_relationship_objects = self.get_customer_subsidiary_relationships(id)
                subsidiaries_already_linked = [relationship.get('subsidiary') for relationship in existing_relationship_objects]
                for relationship in customer_subsidiary_relationships:
                    if relationship.get('subsidiary', {}).get('id') in subsidiaries_already_linked:
                        continue
                    self.logger.info(f"Creating customer subsidiary relationship for customer {id} and subsidiary {relationship.get('subsidiary')}")
                    relationship["entity"] = {"id": id}
                    response = self.rest_post(url=relationship_url, json=relationship)
                    self.logger.info(response)


        elif self.stream_name.lower() in ['item','items']:
            url = f"{self.url_base}"
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
            response = self.rest_post(url=url,json=record)

        if response:
            if name in ["JournalEntry", "CustomerPayment", "InboundShipment"]:
                try:    
                    record_id = response["internalId"]
                except:
                    raise Exception(f"Internal ID not found for {name}, response: {response}")
            else:
                record_id = self._extract_id_from_response_header(response.headers)
            return record_id, True, {}
        else:
            return None, True, {}