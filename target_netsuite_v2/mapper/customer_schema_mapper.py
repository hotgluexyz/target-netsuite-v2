from target_netsuite_v2.mapper.base_mapper import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    """A class responsible for mapping a customer record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id("Customers")
        }

        # Map of record keys to NetSuite payload keys
        field_mappings = {
            "externalId": "externalId"
        }

        # Only add fields that exist in the record
        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload
