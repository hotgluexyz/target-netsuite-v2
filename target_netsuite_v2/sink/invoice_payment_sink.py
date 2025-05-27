from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.invoice_payment_schema_mapper import InvoicePaymentSchemaMapper

class InvoicePaymentSink(NetSuiteBatchSink):
    name = "InvoicePayments"
    record_type = "customerPayment"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, invoice_payments = self.suite_talk_client.get_invoice_payments(
            ids=ids,
            external_ids=external_ids,
            aggregate_payments=False
        )

        invoices_ids = {record["invoiceId"] for record in raw_records if record.get("invoiceId")}
        invoices_external_ids = {record["invoiceExternalId"] for record in raw_records if record.get("invoiceExternalId")}
        _, _, invoices = self.suite_talk_client.get_transaction_data(
            transaction_type="CustInvc",
            external_ids=invoices_external_ids,
            record_ids=invoices_ids,
            extra_select_statement="transaction.entity as entityid"
        )

        customer_ids = {record["customerId"] for record in raw_records if record.get("customerId")}
        customer_external_ids = {record["customerExternalId"] for record in raw_records if record.get("customerExternalId")}
        customer_names = {record["customerName"] for record in raw_records if record.get("customerName")}
        _, _, customers = self.suite_talk_client.get_reference_data(
            "customer",
            record_ids=customer_ids,
            external_ids=customer_external_ids,
            names=customer_names
        )

        return {
            **self._target.reference_data,
            self.name: invoice_payments,
            "Invoices": invoices,
            "Customers": customers,           
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return InvoicePaymentSchemaMapper(record, self.name, None, None, reference_data).to_netsuite()

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        if self.record_exists(record):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            state["is_updated"] = True
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

            if error_message:
                state["error"] = error_message
                return id, success, state

        return id, success, state
