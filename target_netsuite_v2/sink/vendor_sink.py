from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper

class VendorSink(NetSuiteBatchSink):
    name = "Vendors"
    record_type = "vendor"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        ids.update(record["parent"] for record in raw_records if record.get("parent"))
        ids.update(record["parentRef"]["id"] for record in raw_records if record.get("parentRef", {}).get("id"))

        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}

        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, ids)

        return {
            **self._target.reference_data,
            self.name: items,
            "Addresses": addresses
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return VendorSchemaMapper(record, self.name, reference_data).to_netsuite()
