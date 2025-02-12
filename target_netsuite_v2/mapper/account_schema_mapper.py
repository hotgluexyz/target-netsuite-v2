from target_netsuite_v2.mapper.base_mapper import BaseMapper

class AccountSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        return {
            "externalId": self.record.get("externalId"),
            "acctName": self.record.get("name"),
            "description": self.record.get("description"),
            "isInactive": not self.record.get("isActive", True),
            "acctType": self.record.get("type"),
            **self._map_internal_id("Accounts"),
            **self._map_currency(),
            **self._map_subrecord("Accounts", "parent", "parentRef"),
            **self._map_subrecord("Locations", "location", "locationRef"),
            **self._map_subrecord("Classifications", "class", "classRef"),
            **self._map_subrecord("Departments", "department", "departmentRef"),
            **self._map_subrecord_list("Subsidiaries", "subsidiary", "subsidiaryRef")
        }