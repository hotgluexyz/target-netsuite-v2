from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.vendor_credit_line_item_schema_mapper import VendorCreditLineItemSchemaMapper
from target_netsuite_v2.mapper.vendor_credit_expense_line_schema_mapper import VendorCreditExpenseLineSchemaMapper

class VendorCreditSchemaMapper(BaseMapper):
    record_extra_pk_mappings = [
        {"record_field": "vendorCreditNumber", "netsuite_field": "tranId"}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorCreditNumber": "tranId",
        "issueDate": "tranDate",
        "dueDate": "duedate",
        "exchangeRate": "exchangerate",
        "description": "memo",
    }

    def to_netsuite(self) -> dict:

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
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id, external_id_field="locationExternalId"),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id, external_id_field="departmentExternalId"),
            **self._map_line_items(subsidiary_id),
            **self._map_expense_lines(subsidiary_id),
            **self._map_custom_fields()
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
    
    def _map_line_items(self, subsidiary_id):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []
        mapped_tax_lines = []

        for index, line_item in enumerate(line_items):
            payload = VendorCreditLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
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
        
    def _map_expense_lines(self, subsidiary_id):
        expense_lines = self.record.get("expenses", [])
        mapped_expense_lines = []
        mapped_tax_lines = []

        for index, expense_line in enumerate(expense_lines):
            payload = VendorCreditExpenseLineSchemaMapper(expense_line, self.reference_data, subsidiary_id).to_netsuite()
            if tax_code := expense_line.get("taxCode"):
                tax_details_reference = f"NEW_EXPENSE_{index}"
                payload["taxDetailsReference"] = tax_details_reference
                tax_line = self._map_tax_line(tax_details_reference, tax_code, expense_line.get("amount"), expense_line.get("taxAmount"))
                mapped_tax_lines.append(tax_line)
            mapped_expense_lines.append(payload)

        if mapped_expense_lines:
            return { "expense": { "items": mapped_expense_lines }, "expensesTaxDetails": mapped_tax_lines }
        else:
            return {}
    
    def _map_tax_details(self, payload):
        items_tax_details = payload.pop("itemsTaxDetails", [])
        expenses_tax_details = payload.pop("expensesTaxDetails", [])
        tax_details = items_tax_details + expenses_tax_details

        if tax_details:
            payload["taxDetailsOverride"] = True
            payload["taxDetails"] = { "items": tax_details }
