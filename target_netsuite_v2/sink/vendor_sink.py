from target_netsuite_v2.client import NetSuiteSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper


class VendorSink(NetSuiteSink):
    name = "Vendors"
    endpoint = "/vendor"

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("internalId"))

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return VendorSchemaMapper(record, self._target.reference_data).to_netsuite()