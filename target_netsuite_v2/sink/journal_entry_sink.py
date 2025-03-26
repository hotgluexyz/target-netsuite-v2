from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.journal_entry_schema_mapper import JournalEntrySchemaMapper

class JournalEntrySink(NetSuiteBatchSink):
    name = "JournalEntries"
    record_type = "journalEntry"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, journal_entries = self.suite_talk_client.get_transaction_data(
            transaction_type="Journal",
            external_ids=external_ids
        )

        return {
            **self._target.reference_data,
            "JournalEntries": journal_entries
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
