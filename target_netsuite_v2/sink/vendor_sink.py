from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper

class VendorSink(NetSuiteBatchSink):
    name = "Vendors"
    record_type = "vendor"

    def get_batch_reference_data(self, context) -> dict:
        return {
            **self._target.reference_data,
            **self.get_primary_records_for_batch(context)
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return VendorSchemaMapper(record, reference_data).to_netsuite()
