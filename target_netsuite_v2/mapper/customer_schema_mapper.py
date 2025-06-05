from target_netsuite_v2.mapper.base_mapper import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    """A class responsible for mapping a customer record ingested in the unified schema format to a payload for NetSuite"""
    record_extra_pk_mappings = [
        {"record_field": "customerNumber", "netsuite_field": "entityId"}
    ]

    field_mappings = {
        "externalId": "externalId",
        "customerNumber": "entityId",
        "companyName": "companyName",
        "prefix": "salutation",
        "firstName": "firstName",
        "middleName": "middleName",
        "lastName": "lastName",
        "title": "title",
        "email": "email",
        "website": "url",
        "printOnCheckAs": "printOnCheckAs",
        "isPerson": "isPerson"
    }

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        if "subsidiaryId" in self.record or "subsidiaryName" in self.record:
            subsidiary_id = self._find_reference_by_id_or_ref(
                self.reference_data["Subsidiaries"],
                "subsidiaryId",
                "subsidiaryName"
            )["internalId"]
        elif self.existing_record:
            subsidiary_id = self.existing_record["subsidiaryId"]
        else:
            subsidiary_id = None

        payload = {
            **self._map_internal_id(),
            **self._map_subrecord("Customers", "parentId", "parentName", "parent", entity_id_field="parentNumber"),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("CustomerCategory", "categoryId", "categoryName", "category"),
            **self._map_subrecord("Employees", "salesRepId", "salesRepName", "salesRep", subsidiary_scope=subsidiary_id),
            **self._map_custom_fields(),
            **self._map_phone_numbers(),
            **self._map_addressbook(),
            **self._map_currency(),
        }
        self._map_is_active(payload)
        self._map_fields(payload)

        return payload
