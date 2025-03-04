from target_netsuite_v2.mapper.base_mapper import BaseMapper

class InvoiceLineItemSchemaMapper(BaseMapper):
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
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=self.subsidiary_id),
        }

        field_mappings = {
            "quantity": "quantity",
            "unitPrice": "rate",
            "totalPrice": "amount",
            "description": "description"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload
