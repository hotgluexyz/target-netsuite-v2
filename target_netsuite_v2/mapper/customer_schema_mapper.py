from target_netsuite_v2.mapper.base_mapper import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    """A class responsible for mapping a customer record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        subsidiary_id = self._find_subsidiary("subsidiaryId", "subsidiaryName").get("internalId")

        payload = {
            **self._map_internal_id(),
            **self._map_subrecord("Customers", "parentId", "parentName", "parent"),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("CustomerCategory", "categoryId", "categoryName", "category"),
            **self._map_subrecord("Employees", "salesRepId", "salesRepName", "salesRep", subsidiary_scope=subsidiary_id),
            **self._map_custom_fields(),
            **self._map_phone_numbers(),
            **self._map_addressbook(),
            **self._map_currency(),
        }

        field_mappings = {
            "externalId": "externalId",
            "companyName": "companyName",
            "salutation": "salutation",
            "firstName": "firstName",
            "middleName": "middleName",
            "lastName": "lastName",
            "title": "title",
            "email": "email",
            "website": "url",
            "printOnCheckAs": "printOnCheckAs"
        }

        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload
