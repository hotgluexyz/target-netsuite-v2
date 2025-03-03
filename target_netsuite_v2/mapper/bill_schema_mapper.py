from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.bill_line_item_schema_mapper import BillLineItemSchemaMapper
from target_netsuite_v2.mapper.bill_expense_schema_mapper import BillExpenseSchemaMapper

class BillSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id(),
            **self._map_entity(),
            **self._map_currency(),
            **self._map_custom_fields(),
            **self._map_subrecord("Locations", "locationId", "locationName", "location"),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_bill_line_items(),
            **self._map_bill_expenses()
        }

        field_mappings = {
            "externalId": "externalId",
            "externalId": "tranId",
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

    def _map_bill_line_items(self):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []

        for line_item in line_items:
            payload = BillLineItemSchemaMapper(line_item, self.reference_data).to_netsuite()
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "item": { "items": mapped_line_items } }
        else:
            return {}

    def _map_bill_expenses(self):
        expenses = self.record.get("expenses", [])
        mapped_expenses = []

        for expense in expenses:
            payload =  BillExpenseSchemaMapper(expense, self.reference_data).to_netsuite()
            mapped_expenses.append(payload)

        if mapped_expenses:
            return { "expense": { "items": mapped_expenses } }
        else:
            return {}
