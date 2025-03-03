from target_netsuite_v2.mapper.base_mapper import BaseMapper

class VendorSchemaMapper(BaseMapper):
    """A class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id(),
            **self._map_phone_numbers(),
            **self._map_addresses(),
            **self._map_currency(),
            **self._map_subrecord("VendorCategory", "categoryId", "categoryName", "category"),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary")
        }

        field_mappings = {
            "externalId": "externalId",
            "vendorName": "companyName",
            "prefix": "salutation",
            "firstName": "firstName",
            "middleName": "middleName",
            "lastName": "lastName",
            "title": "title",
            "email": "email",
            "website": "url",
            "checkName": "printOnCheckAs",
            "balance": "balance",
            "updatedAt": "lastModifiedDate",
            "createdAt": "dateCreated"
        }

        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload