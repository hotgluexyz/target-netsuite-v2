from hotglue_models_accounting.accounting import Item
from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.item_schema_mapper import ItemSchemaMapper

class ItemSink(NetSuiteBatchSink):
    name = "Items"
    record_type = "item"
    unified_schema = Item
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        item_ids = {record["itemNumber"] for record in raw_records if record.get("itemNumber")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        names = {record["name"] for record in raw_records if record.get("name")}
        names.update(record["displayName"] for record in raw_records if record.get("displayName"))
        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids,
            names=names,
            item_ids=item_ids
        )

        return {
            **self._target.reference_data,
            "Items": items
        }

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        did_update = False
        if self.record_exists(record):
            id, success, error_message = self.suite_talk_client.update_item(record['internalId'], record)
            did_update = True
        else:
            id, success, error_message = self.suite_talk_client.create_item(record)

        if error_message:
            state["error"] = error_message
        elif did_update:
            state["is_updated"] = True

        return id, success, state

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return ItemSchemaMapper(record, self.name, reference_data).to_netsuite()
