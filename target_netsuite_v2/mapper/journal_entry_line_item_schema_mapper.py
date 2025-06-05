from target_netsuite_v2.mapper.base_mapper import BaseMapper

class JournalEntryLineItemSchemaMapper(BaseMapper):
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
            **self._map_subrecord("Accounts", "accountId", "accountName", "account"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=self.subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=self.subsidiary_id),
            **self._map_entity(),
            **self._map_credit_debit()
        }

        field_mappings = {
            "description": "memo"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload

    def _map_credit_debit(self):
        if self.record.get("entryType") == "Credit":
            return {"credit": self.record.get("creditAmount", None)}
        
        return {"debit": self.record.get("debitAmount", None)}

    def _map_entity(self):
        found_vendor = self._find_reference_by_id_or_ref(
            self.reference_data["Vendors"],
            "vendorId",
            "vendorName",
            external_id_field="vendorExternalId",
            entity_id_field="vendorNumber"
        )

        found_customer = self._find_reference_by_id_or_ref(
            self.reference_data["Customers"],
            "customerId",
            "customerName",
            external_id_field="customerExternalId",
            entity_id_field="customerNumber"
        )
        entity = found_customer or found_vendor

        return {"entity": {"id": entity["internalId"] } } if entity else {}
