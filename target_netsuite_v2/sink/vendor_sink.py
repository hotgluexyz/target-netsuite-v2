from target_netsuite_v2.sinks import NetSuiteSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper

class VendorSink(NetSuiteSink):
    name = "Vendors"
    endpoint = "/vendor"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return VendorSchemaMapper(record, self._target.reference_data).to_netsuite()