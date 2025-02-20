from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper

class VendorSink(NetSuiteBatchSink):
    name = "Vendors"
    record_type = "vendor"

    def get_batch_reference_data(self, context) -> dict:
        return {
            **self._target.reference_data,
            **self.get_primary_records_for_batch(context),
            **self.get_addresses_for_batch(context)
        }

    def get_addresses_for_batch(self, context) -> dict:
        raw_records = context["records"]

        ids = set()

        for record in raw_records:
            if record.get("id"):
                ids.add(record["id"])

        _, _, addresses = self.suite_talk_client.get_customer_default_addresses(list(ids))

        return {
            "Addresses": addresses
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return VendorSchemaMapper(record, reference_data).to_netsuite()
