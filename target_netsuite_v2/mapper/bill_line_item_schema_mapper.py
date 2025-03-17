from target_netsuite_v2.mapper.base_mapper import BaseMapper

class BillLineItemSchemaMapper(BaseMapper):
    field_mappings = {
        "description": "description",
        "quantity": "quantity",
        "unitPrice": "rate",
        "totalPrice": "amount",
    }

    def __init__(
            self,
            record,
            reference_data,
            subsidiary_id
    ) -> None:
        self.record = record
        self.reference_data = reference_data
        self.subsidiary_id = subsidiary_id

    def to_netsuite(self) -> dict:
        payload = {
            **self._map_custom_fields(),
            **self._map_subrecord("Items", "itemId", "itemName", "item"),
            **self._map_subrecord("Accounts", "accountId", "accountName", "account"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=self.subsidiary_id),
        }

        self._map_fields(payload)

        return payload
