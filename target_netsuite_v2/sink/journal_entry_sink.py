from hotglue_models_accounting.accounting import JournalEntry
from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.journal_entry_schema_mapper import JournalEntrySchemaMapper

class JournalEntrySink(NetSuiteBatchSink):
    name = "JournalEntries"
    record_type = "journalEntry"
    unified_schema = JournalEntry
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        tran_ids = {record["journalEntryNumber"] for record in raw_records if record.get("journalEntryNumber")}
        ids = {record["id"] for record in raw_records if record.get("id")}
        _, _, journal_entries = self.suite_talk_client.get_transaction_data(
            transaction_type="Journal",
            external_ids=external_ids,
            record_ids=ids,
            tran_ids=tran_ids
        )

        customer_ids = set()
        customer_entity_ids = set()
        customer_names = set()
        for record in raw_records:
            customer_ids.update(line_item["customerId"] for line_item in record.get("lineItems", []) if line_item.get("customerId"))
            customer_entity_ids.update(line_item["customerNumber"] for line_item in record.get("lineItems", []) if line_item.get("customerNumber"))
            customer_names.update(line_item["customerName"] for line_item in record.get("lineItems", []) if line_item.get("customerName"))
        _, _, customers = self.suite_talk_client.get_reference_data(
            "customer",
            record_ids=customer_ids,
            names=customer_names,
            entity_ids=customer_entity_ids
        )

        vendor_ids = set()
        vendor_entity_ids = set()
        vendor_names = set()
        for record in raw_records:
            vendor_ids.update(line_item["vendorId"] for line_item in record.get("lineItems", []) if line_item.get("vendorId"))
            vendor_entity_ids.update(line_item["vendorNumber"] for line_item in record.get("lineItems", []) if line_item.get("vendorNumber"))
            vendor_names.update(line_item["vendorName"] for line_item in record.get("lineItems", []) if line_item.get("vendorName"))
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            names=vendor_names,
            entity_ids=vendor_entity_ids
        )

        return {
            **self._target.reference_data,
            "JournalEntries": journal_entries,
            "Customers": customers,
            "Vendors": vendors
        }

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        did_update = False
        if self.record_exists(record):
            id, success, error_message = (None, False, "Record already exists")
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

        if error_message:
            state["error"] = error_message

        return id, success, state

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return JournalEntrySchemaMapper(record, self.name, reference_data).to_netsuite()
