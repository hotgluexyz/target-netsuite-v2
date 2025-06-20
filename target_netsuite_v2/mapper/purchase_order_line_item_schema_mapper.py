from target_netsuite_v2.mapper.base_mapper import BaseMapper

class PurchaseOrderLineItemSchemaMapper(BaseMapper):
    field_mappings = {
        "quantity": "quantity",
        "unitPrice": "rate",
        "description": "description"
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
        if "subsidiaryId" in self.record or "subsidiaryName" in self.record:
            subsidiary_id = self._find_reference_by_id_or_ref(
                self.reference_data["Subsidiaries"],
                "subsidiaryId",
                "subsidiaryName"
            )["internalId"]
        else:
            subsidiary_id = self.subsidiary_id

        payload = {
            **self._map_custom_fields(),
            **self._map_subrecord("Customers", "projectId", "projectName", "customer", external_id_field="projectExternalId", entity_id_field="projectNumber"),
            **self._map_item(),
            **self._map_subrecord("Classifications", "classId", "className", "class", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Departments", "departmentId", "departmentName", "department", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_subrecord("Employees", "employeeId", "employeeName", "employee", subsidiary_scope=self.subsidiary_id, external_id_field="employeeNumber")
        }

        self._map_fields(payload)

        return payload
