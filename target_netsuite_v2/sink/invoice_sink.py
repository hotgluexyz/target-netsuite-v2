from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.invoice_schema_mapper import InvoiceSchemaMapper

class InvoiceSink(NetSuiteBatchSink):
    name = "Invoices"
    record_type = "invoice"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, invoices = self.suite_talk_client.get_transaction_data(
            transaction_type="CustInvc",
            external_ids=external_ids
        )

        customer_ids = {record["customerId"] for record in raw_records if record.get("customerId")}
        customer_names = {record["customerName"] for record in raw_records if record.get("customerName")}
        _, _, customers = self.suite_talk_client.get_reference_data(
            "customer",
            record_ids=customer_ids,
            names=customer_names
        )

        item_ids = set()
        item_names = set()
        for record in raw_records:
            item_ids.update(line_item["itemId"] for line_item in record.get("lineItems", []) if line_item.get("itemId"))
            item_names.update(line_item["itemName"] for line_item in record.get("lineItems", []) if line_item.get("itemName"))
        _, _, items = self.suite_talk_client.get_reference_data(
            "item",
            record_ids = item_ids,
            names = item_names
        )

        # _, _, invoice_items = self.suite_talk_client.get_invoice_items(
        #     external_ids=external_ids
        # )

        # invoice_ids = {invoice["internalId"] for invoice in invoices}
        # _, _, invoice_payments = self.suite_talk_client.get_invoice_payments(
        #     invoice_ids=invoice_ids
        # )

        return {
            **self._target.reference_data,
            "Invoices": invoices,
            "Customers": customers,
            "Items": items,
            # "InvoiceItems": invoice_items,
            # "InvoicePayments": invoice_payments
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return InvoiceSchemaMapper(record, self.name, reference_data).to_netsuite()
