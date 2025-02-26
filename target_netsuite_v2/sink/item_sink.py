from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.item_schema_mapper import ItemSchemaMapper

class ItemSink(NetSuiteBatchSink):
    name = "Items"
    record_type = "item"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        names = {record["name"] for record in raw_records if record.get("name")}
        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids,
            names=names
        )

        return {
            **self._target.reference_data,
            "Items": items
        }

    def upsert_record(self, record: dict, context: dict):
        state = {}

        if self.record_exists(record, context):
            id, success, error_message = self.suite_talk_client.update_item(record['internalId'], record)
        else:
            id, success, error_message = self.suite_talk_client.create_item(record)

        if error_message:
            state["error"] = error_message

        return id, success, state

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return ItemSchemaMapper(record, self.name, reference_data).to_netsuite()
