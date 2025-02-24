from target_netsuite_v2.mapper.base_mapper import BaseMapper

class ItemSchemaMapper(BaseMapper):
    """A class responsible for mapping an item record ingested in the unified schema format to a payload for NetSuite"""

    def _map_accounts(self):
        accounts = self.record.get("accounts", [])

        account_dict = {}

        for account in accounts:
            id = account["id"]
            account_dict_key = f"{account['accountType']}Account"
            account_dict[account_dict_key] = { "id": id }

        return account_dict

    def to_netsuite(self) -> dict:
        payload = {
            **self._map_internal_id(),
            **self._map_subrecord_list("Subsidiaries", "subsidiary", "subsidiaryRef"),
            **self._map_subrecord("Locations", "location", "locationRef"),
            **self._map_subrecord("Classifications", "class", "classRef"),
            **self._map_subrecord("Departments", "department", "departmentRef"),
            **self._map_accounts()
        }
        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        field_mappings = {
            "code": "itemId",
            "displayName": "displayName",
            "type": "type",
            "category": "category"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload