from target_netsuite_v2.mapper.base_mapper import BaseMapper

class AccountSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""

    field_mappings = {
        "externalId": "externalId",
        "name": "acctName",
        "description": "description",
        "type": "acctType"
    }

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        subsidiary_id = self._find_subsidiaries("subsidiary", "subsidiaryRef")[0].get("internalId")

        payload = {
            **self._map_internal_id(),
            **self._map_currency(),
            **self._map_subrecord("Accounts", "parentId", "parentName", "parent"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id),
            **self._map_subrecord_list("Subsidiaries", "subsidiary", "subsidiaryRef")
        }
        self._map_is_active(payload)
        self._map_fields(payload)

        return payload