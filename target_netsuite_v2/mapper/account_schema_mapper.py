from target_netsuite_v2.mapper.base_mapper import BaseMapper

class AccountSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id(),
            **self._map_currency(),
            **self._map_subrecord("Accounts", "parentId", "parentName", "parent"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location"),
            **self._map_subrecord("Classifications", "classId", "className", "class"),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department"),
            **self._map_subrecord_list("Subsidiaries", "subsidiary", "subsidiaryRef")
        }

        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        field_mappings = {
            "externalId": "externalId",
            "name": "acctName",
            "description": "description",
            "type": "acctType"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload