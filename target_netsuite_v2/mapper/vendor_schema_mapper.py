from target_netsuite_v2.mapper.base_mapper import BaseMapper

class VendorSchemaMapper(BaseMapper):
    """A class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""
    record_extra_pk_mappings = [
        {"record_field": "vendorNumber", "netsuite_field": "entityId"}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorNumber": "entityId",
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
        "createdAt": "dateCreated",
        "isPerson": "isPerson"
    }

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id(),
            **self._map_phone_numbers(),
            **self._map_addressbook(),
            **self._map_currency(),
            **self._map_subrecord("VendorCategory", "categoryId", "categoryName", "category"),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_custom_fields()
        }
        self._map_is_active(payload)
        self._map_fields(payload)

        return payload