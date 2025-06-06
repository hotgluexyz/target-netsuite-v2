from target_netsuite_v2.mapper.base_mapper import BaseMapper, InvalidAccountError

class ItemSchemaMapper(BaseMapper):
    """A class responsible for mapping an item record ingested in the unified schema format to a payload for NetSuite"""
    record_extra_pk_mappings = [
        {"record_field": "itemNumber", "netsuite_field": "itemId"}
    ]

    field_mappings = {
        "itemNumber": "itemId",
        "displayName": "displayName",
        "type": "type",
        "category": "category",
        "externalId": "externalId"
    }

    def _map_accounts(self):
        accounts = self.record.get("accounts", [])

        account_dict = {}

        for account in accounts:
            if "id" not in account or "accountType" not in account:
                raise InvalidAccountError("Invalid account provided. Must include 'id' and 'accountType' fields.")
            id = account["id"]
            account_dict_key = f"{account['accountType']}Account"
            account_dict[account_dict_key] = { "id": id }

        return account_dict

    def to_netsuite(self) -> dict:
        subsidiary_id = self._find_subsidiaries("subsidiary", "subsidiaryRef")[0].get("internalId")

        payload = {
            **self._map_internal_id(),
            **self._map_subrecord_list("Subsidiaries", "subsidiary", "subsidiaryRef"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id),
            **self._map_accounts(),
            **self._map_custom_fields()
        }
        self._map_is_active(payload)
        self._map_fields(payload)

        return payload