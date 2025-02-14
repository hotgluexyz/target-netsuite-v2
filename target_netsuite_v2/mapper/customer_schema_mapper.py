from target_netsuite_v2.mapper.base_mapper import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    """A class responsible for mapping a customer record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        return {
            "externalId": self.record.get("externalId"),
            **self._map_internal_id("Customers"),
        }
