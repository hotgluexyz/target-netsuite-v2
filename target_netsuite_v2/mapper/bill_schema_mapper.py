from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.bill_line_item_schema_mapper import BillLineItemSchemaMapper
from target_netsuite_v2.mapper.bill_expense_schema_mapper import BillExpenseSchemaMapper

class BillSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    record_extra_pk_mappings = [
        {"record_field": "billNumber", "netsuite_field": "tranId"}
    ]
    
    field_mappings = {
        "externalId": "externalId",
        "billNumber": "tranId",
        "dueDate": "dueDate",
        "paidDate": "enddate",
        "balance": "balance",
        "totalAmount": "total",
        "issueDate": "tranDate",
        "exchangeRate": "exchangeRate",
        "relatedPayments": "relatedPayments",
        "description": "memo",
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
            **self._map_entity(),
            **self._map_currency(),
            **self._map_custom_fields(),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id),
            **self._map_bill_line_items(subsidiary_id),
            **self._map_bill_expenses(subsidiary_id)
        }

        self._map_tax_details(payload)
        self._map_fields(payload)

        return payload

    def _map_entity(self):
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Vendors"],
            "vendorId",
            "vendorName",
            external_id_field="vendorExternalId",
            entity_id_field="vendorNumber"
        )

        if reference:
            return { "entity": { "id": reference["internalId"] } }

        return {}

    def _map_bill_line_items(self, subsidiary_id):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []
        mapped_tax_lines = []

        for index, line_item in enumerate(line_items):
            payload = BillLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
            if tax_code := line_item.get("taxCode"):
                tax_details_reference = f"NEW_ITEM_{index}"
                payload["taxDetailsReference"] = tax_details_reference
                tax_line = self._map_tax_line(tax_details_reference, tax_code, line_item.get("amount"), line_item.get("taxAmount"))
                mapped_tax_lines.append(tax_line)
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "item": { "items": mapped_line_items }, "itemsTaxDetails": mapped_tax_lines }
        else:
            return {}

    def _map_bill_expenses(self, subsidiary_id):
        expenses = self.record.get("expenses", [])
        mapped_expenses = []
        mapped_tax_lines = []

        for index, expense in enumerate(expenses):
            payload =  BillExpenseSchemaMapper(expense, self.reference_data, subsidiary_id).to_netsuite()
            if tax_code := expense.get("taxCode"):
                tax_details_reference = f"NEW_EXPENSE_{index}"
                payload["taxDetailsReference"] = tax_details_reference
                tax_line = self._map_tax_line(tax_details_reference, tax_code, expense.get("amount"), expense.get("taxAmount"))
                mapped_tax_lines.append(tax_line)
            mapped_expenses.append(payload)

        if mapped_expenses:
            return { "expense": { "items": mapped_expenses }, "expensesTaxDetails": mapped_tax_lines }
        else:
            return {}

    def _map_tax_details(self, payload):
        items_tax_details = payload.pop("itemsTaxDetails", [])
        expenses_tax_details = payload.pop("expensesTaxDetails", [])
        tax_details = items_tax_details + expenses_tax_details

        if tax_details:
            payload["taxDetailsOverride"] = True
            payload["taxDetails"] = { "items": tax_details }
