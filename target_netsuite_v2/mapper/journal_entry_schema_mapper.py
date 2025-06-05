from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.journal_entry_line_item_schema_mapper import JournalEntryLineItemSchemaMapper

class JournalEntrySchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    record_extra_pk_mappings = [
        {"record_field": "journalEntryNumber", "netsuite_field": "tranId"}
    ]

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        subsidiary = self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary")
        subsidiary_id = subsidiary.get("subsidiary").get("id")

        payload = {
            **self._map_internal_id(),
            **self._map_currency(),
            **subsidiary,
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id),
            **self._map_custom_fields(),
            **self._map_journal_entry_line_items(subsidiary_id)
        }

        field_mappings = {
            "externalId": "externalId",
            "journalEntryNumber": "tranId",
            "transactionDate": "tranDate",
            "description": "memo",
            "exchangeRate": "exchangeRate",
            "postingPeriod": "postingPeriod"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload
    
    def _map_journal_entry_line_items(self, subsidiary_id):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []

        for line_item in line_items:
            payload = JournalEntryLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "line": { "items": mapped_line_items } }
        else:
            return {}