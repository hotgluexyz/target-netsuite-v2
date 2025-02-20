from target_netsuite_v2.mapper.base_mapper import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    """A class responsible for mapping a customer record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id(),
            **self._map_subrecord("Customers", "parent", "parentRef"),
            **self._map_subrecord("Subsidiaries", "subsidiary", "subsidiaryRef"),
            **self._map_subrecord("CustomerCategory", "category", "categoryRef"),
            **self._map_subrecord("Employees", "salesRep", "salesRepRef"),
            **self._map_custom_fields(),
            **self._map_phone_numbers(),
            **self._map_addresses(),
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
            "url": "url",
            "printOnCheckAs": "printOnCheckAs"
        }

        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload
