from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.purchase_order_line_item_schema_mapper import PurchaseOrderLineItemSchemaMapper

class PurchaseOrderSchemaMapper(BaseMapper):
    record_extra_pk_mappings = [
        {"record_field": "purchaseOrderNumber", "netsuite_field": "tranId"}
    ]

    field_mappings = {
        "externalId": "externalId",
        "purchaseOrderNumber": "tranId",
        "description": "memo",
        "exchangeRate": "exchangeRate",
        "dueDate": "dueDate",
        "issueDate": "tranDate",
        "paidDate": "endDate"
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
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("Employees", "employeeId", "employeeName", "employee", subsidiary_scope=subsidiary_id, external_id_field="employeeExternalId"),
            **self._map_line_items(subsidiary_id)
        }

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

        for line_item in line_items:
            payload = PurchaseOrderLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "item": { "items": mapped_line_items } }
        else:
            return {}
