from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.invoice_schema_mapper import InvoiceSchemaMapper
from target_netsuite_v2.mapper.invoice_payment_schema_mapper import InvoicePaymentSchemaMapper
from target_netsuite_v2.mapper.base_mapper import InvalidReferenceError

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

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        _record = self._omit_key(record, "relatedPayments")

        if self.record_exists(record):
            post_processed_record = self.post_processing_for_update(_record, reference_data)
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, _record['internalId'], post_processed_record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            _, success, error_messages = self.create_child_records(id, record, reference_data)

            if error_messages:
                state["error"] = error_messages
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, _record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            _, success, error_messages = self.create_child_records(id, record, reference_data)

            if error_messages:
                state["error"] = error_messages

        return id, success, state

    def create_child_records(self, parent_id: int, record: dict, reference_data: dict):
        payments = record.get("relatedPayments", [])

        created_ids = []
        error_messages = []
        for payment in payments:
            # if self.check_payment_exists(parent_id, payment, reference_data):
            #     continue

            try:
                preprocessed_payment = InvoicePaymentSchemaMapper(payment, record.get("entity"), parent_id, reference_data).to_netsuite()
                id, success, error_message = self.suite_talk_client.create_record("customerPayment", preprocessed_payment)
                if not success:
                    error_messages.append(f"Error creating payment for Invoice: {error_message}")
                else:
                    created_ids.append(id)
            except InvalidReferenceError as e:
                error_messages.append(f"Error creating payment for Invoice: {str(e)}")

        return created_ids, len(error_messages) == 0, error_messages
