from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper

class VendorSink(NetSuiteBatchSink):
    name = "Vendors"
    record_type = "vendor"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        entity_ids = {record["vendorNumber"] for record in raw_records if record.get("vendorNumber")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        names = {record["vendorName"] for record in raw_records if record.get("vendorName")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids,
            names=names,
            entity_ids=entity_ids
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, {vendor["internalId"] for vendor in vendors})

        return {
            **self._target.reference_data,
            self.name: vendors,
            "Addresses": addresses
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return VendorSchemaMapper(record, self.name, reference_data).to_netsuite()
