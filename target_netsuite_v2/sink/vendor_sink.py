from target_netsuite_v2.client import NetSuiteSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper


class VendorSink(NetSuiteSink):
    name = "Vendors"
    endpoint = "/vendor"

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("id"))

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return VendorSchemaMapper(record, context).to_netsuite()