from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.bill_payment_schema_mapper import BillPaymentSchemaMapper
from target_netsuite_v2.mapper.base_mapper import InvalidInputError

class BillPaymentSink(NetSuiteBatchSink):
    name = "BillPayments"
    record_type = "vendorPayment"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        tran_ids = {record["paymentNumber"] for record in raw_records if record.get("paymentNumber")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, bill_payments = self.suite_talk_client.get_bill_payments(
            ids=ids,
            external_ids=external_ids,
            tran_ids=tran_ids,
            aggregate_payments=False
        )

        bills_ids = {record["billId"] for record in raw_records if record.get("billId")}
        bills_tran_ids = {record["billNumber"] for record in raw_records if record.get("billNumber")}
        bills_external_ids = {record["billExternalId"] for record in raw_records if record.get("billExternalId")}
        _, _, bills = self.suite_talk_client.get_transaction_data(
            transaction_type="VendBill",
            external_ids=bills_external_ids,
            record_ids=bills_ids,
            tran_ids=bills_tran_ids,
            extra_select_statement="transaction.entity as entityid"
        )

        vendor_ids = {record["vendorId"] for record in raw_records if record.get("vendorId")}
        vendor_entity_ids = {record["vendorNumber"] for record in raw_records if record.get("vendorNumber")}
        vendor_external_ids = {record["vendorExternalId"] for record in raw_records if record.get("vendorExternalId")}
        vendor_names = {record["vendorName"] for record in raw_records if record.get("vendorName")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            external_ids=vendor_external_ids,
            names=vendor_names,
            entity_ids=vendor_entity_ids
        )

        return {
            **self._target.reference_data,
            self.name: bill_payments,
            "Bills": bills,
            "Vendors": vendors            
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return BillPaymentSchemaMapper(record, self.name, None, None, reference_data).to_netsuite()

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
