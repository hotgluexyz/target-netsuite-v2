from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.bill_schema_mapper import BillSchemaMapper
from target_netsuite_v2.mapper.bill_payment_schema_mapper import BillPaymentSchemaMapper

class BillSink(NetSuiteBatchSink):
    name = "Bills"
    record_type = "vendorBill"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        # ids = {record["id"] for record in raw_records if record.get("id")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, bills = self.suite_talk_client.get_transaction_data(
            tran_ids=external_ids
        )

        vendor_ids = {record["vendor"] for record in raw_records if record.get("vendor")}
        vendor_ids.update(record["vendorRef"]["id"] for record in raw_records if record.get("vendorRef", {}).get("id"))
        vendor_names = {record["vendorRef"]["name"] for record in raw_records if record.get("vendorRef", {}).get("name")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            names=vendor_names
        )

        item_ids = set()
        item_names = set()
        for record in raw_records:
            item_ids.update(line_item["item"] for line_item in record.get("lineItems", []) if line_item.get("item"))
            item_ids.update(line_item["itemRef"]["id"] for line_item in record.get("lineItems", []) if line_item.get("itemRef", {}).get("id"))
            item_names.update(line_item["itemRef"]["name"] for line_item in record.get("lineItems", []) if line_item.get("itemRef", {}).get("name"))
        _, _, items = self.suite_talk_client.get_reference_data(
            "item",
            record_ids = item_ids,
            names = item_names
        )

        _, _, bill_items = self.suite_talk_client.get_bill_items(
            external_ids=external_ids
        )

        return {
            **self._target.reference_data,
            "Bills": bills,
            "Vendors": vendors,
            "Items": items,
            "BillItems": bill_items
        }

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        _record = self._omit_key(record, "relatedPayments")

        if self.record_exists(record):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], _record)
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, _record)

            if error_message:
                state["error"] = error_message
                return

            _, _, error_message = self.create_child_records(id, record, reference_data)

            # TODO: enrich error message with the payment that failed
            if error_message:
                state["error"] = error_message

        return id, success, state

    def create_child_records(self, parent_id: int, record: dict, reference_data: dict):
        payments = record.get("relatedPayments", [])

        created_ids = []
        for payment in payments:
            preprocessed_payment = BillPaymentSchemaMapper(payment, record.get("entity"), parent_id, reference_data).to_netsuite()

            id, success, error_message = self.suite_talk_client.create_record("vendorPayment", preprocessed_payment)

            if not success:
                return created_ids, False, error_message,

            created_ids.append(id)

        return created_ids, True, None

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return BillSchemaMapper(record, self.name, reference_data).to_netsuite()

    def _omit_key(self, d, key):
        return {k: v for k, v in d.items() if k != key}

