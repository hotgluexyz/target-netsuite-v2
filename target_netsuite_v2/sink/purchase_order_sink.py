from hotglue_models_accounting.accounting import PurchaseOrder
from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.purchase_order_schema_mapper import PurchaseOrderSchemaMapper
from target_netsuite_v2.mapper.base_mapper import InvalidInputError

class PurchaseOrderSink(NetSuiteBatchSink):
    name = "PurchaseOrders"
    record_type = "purchaseOrder"
    unified_schema = PurchaseOrder
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        tran_ids = {record["purchaseOrderNumber"] for record in raw_records if record.get("purchaseOrderNumber")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, purchase_orders = self.suite_talk_client.get_transaction_data(
            transaction_type="PurchOrd",
            external_ids=external_ids,
            record_ids=ids,
            tran_ids=tran_ids
        )

        vendor_ids = {record["vendorId"] for record in raw_records if record.get("vendorId")}
        vendor_external_ids = {record["vendorExternalId"] for record in raw_records if record.get("vendorExternalId")}
        vendor_numbers = {record["vendorNumber"] for record in raw_records if record.get("vendorNumber")}
        vendor_names = {record["vendorName"] for record in raw_records if record.get("vendorName")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            external_ids=vendor_external_ids,
            names=vendor_names,
            entity_ids=vendor_numbers
        )

        employee_ids = {record["employeeId"] for record in raw_records if record.get("employeeId")}
        employee_external_ids = {record["employeeExternalId"] for record in raw_records if record.get("employeeExternalId")}
        employee_names = {record["employeeName"] for record in raw_records if record.get("employeeName")}
        _, _, employees = self.suite_talk_client.get_reference_data(
            "employee",
            record_ids=employee_ids,
            names=employee_names,
            external_ids=employee_external_ids
        )

        customer_ids = set()
        customer_names = set()
        customer_entity_ids = set()
        customer_external_ids = set()
        for record in raw_records:
            customer_ids.update(line_item["projectId"] for line_item in record.get("lineItems", []) if line_item.get("projectId"))
            customer_names.update(line_item["projectName"] for line_item in record.get("lineItems", []) if line_item.get("projectName"))
            customer_entity_ids.update(line_item["projectNumber"] for line_item in record.get("lineItems", []) if line_item.get("projectNumber"))
            customer_external_ids.update(line_item["projectExternalId"] for line_item in record.get("lineItems", []) if line_item.get("projectExternalId"))
        _, _, customers = self.suite_talk_client.get_reference_data(
            "customer",
            record_ids = customer_ids,
            names = customer_names,
            external_ids = customer_external_ids,
            entity_ids=customer_entity_ids
        )

        item_record_ids = set()
        item_ids = set()
        item_names = set()
        item_external_ids = set()
        for record in raw_records:
            item_record_ids.update(line_item["itemId"] for line_item in record.get("lineItems", []) if line_item.get("itemId"))
            item_ids.update(line_item["itemNumber"] for line_item in record.get("lineItems", []) if line_item.get("itemNumber"))
            item_names.update(line_item["itemName"] for line_item in record.get("lineItems", []) if line_item.get("itemName"))
            item_external_ids.update(line_item["itemExternalId"] for line_item in record.get("lineItems", []) if line_item.get("itemExternalId"))
        _, _, items = self.suite_talk_client.get_reference_data(
            "item",
            record_ids = item_record_ids,
            names = item_names,
            external_ids = item_external_ids,
            item_ids=item_ids
        )

        purchase_order_ids = {purchase_order["internalId"] for purchase_order in purchase_orders}
        _, _, purchase_order_items = self.suite_talk_client.get_purchase_order_items(
            purchase_order_ids
        )

        return {
            **self._target.reference_data,
            self.name: purchase_orders,
            "PurchaseOrderItems": purchase_order_items,
            "Vendors": vendors,
            "Employees": employees,
            "Customers": customers,
            "Items": items
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return PurchaseOrderSchemaMapper(record, self.name, reference_data).to_netsuite()

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        if self.record_exists(record):
            post_processed_record = self.post_processing_for_update(record, reference_data)
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], post_processed_record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            state["is_updated"] = True
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

            if error_message:
                state["error"] = error_message
                return id, success, state

        return id, success, state

    def post_processing_for_update(self, record, reference_data):
        items = record.get("item", {}).get("items", [])
        new_items = []
        for item in items:
            exists = self.check_item_exists(record['internalId'], item, reference_data)
            if not exists:
                new_items.append(item)

        if new_items:
            new_item_payload = {
                "items": new_items
            }
            record["item"] = new_item_payload
        else:
            record = self._omit_key(record, "item")

        return record

    def check_item_exists(self, record_id, item, reference_data):
        existing_items = reference_data["PurchaseOrderItems"].get(record_id, {}).get("lineItems", [])
        for existing_item in existing_items:
            does_exist = self.compare_item(existing_item, item)
            if does_exist:
                return True
        return False

    def compare_item(self, existing_item, new_item):
        if existing_item.get("memo") == new_item.get("description") and existing_item.get("memo") != None:
            return True
        return False
