from target_netsuite_v2.mapper.base_mapper import BaseMapper

class BillLineItemSchemaMapper(BaseMapper):
    def __init__(
            self,
            record,
            reference_data
    ) -> None:
        self.record = record
        self.reference_data = reference_data

    def to_netsuite(self) -> dict:
        payload = {
            **self._map_custom_fields(),
            **self._map_subrecord("Items", "item", "itemRef"),
            **self._map_subrecord("Accounts", "account", "accountRef"),
            **self._map_subrecord("Locations", "location", "locationRef"),
            **self._map_subrecord("Classifications", "class", "classRef"),
            **self._map_subrecord("Departments", "department", "departmentRef"),
        }

        field_mappings = {
            "description": "description",
            "quantity": "quantity",
            "unitPrice": "rate",
            "totalPrice": "amount",
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)
        return payload

