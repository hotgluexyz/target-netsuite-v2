from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.bill_line_item_schema_mapper import BillLineItemSchemaMapper
from target_netsuite_v2.mapper.bill_expense_schema_mapper import BillExpenseSchemaMapper

class BillSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""

        subsidiary_id = self._find_subsidiary().get("internalId")

        payload = {
            **self._map_internal_id(),
            **self._map_entity(),
            **self._map_currency(),
            **self._map_custom_fields(),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_bill_line_items(subsidiary_id),
            **self._map_bill_expenses(subsidiary_id)
        }

        field_mappings = {
            "externalId": "externalId",
            "dueDate": "dueDate",
            "balance": "balance",
            "totalAmount": "total",
            "issueDate": "tranDate",
            "exchangeRate": "exchangeRate",
            "relatedPayments": "relatedPayments"
        }

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        return payload

    def _map_entity(self):
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Vendors"],
            "vendorId",
            "vendorName"
        )

        if reference:
            return { "entity": { "id": reference["internalId"] } }

        return {}

    def _map_bill_line_items(self, subsidiary_id):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []

        for line_item in line_items:
            payload = BillLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "item": { "items": mapped_line_items } }
        else:
            return {}

    def _map_bill_expenses(self, subsidiary_id):
        expenses = self.record.get("expenses", [])
        mapped_expenses = []

        for expense in expenses:
            payload =  BillExpenseSchemaMapper(expense, self.reference_data, subsidiary_id).to_netsuite()
            mapped_expenses.append(payload)

        if mapped_expenses:
            return { "expense": { "items": mapped_expenses } }
        else:
            return {}
