from hotglue_models_accounting.accounting import VendorCredit
from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.vendor_credit_schema_mapper import VendorCreditSchemaMapper

class VendorCreditSink(NetSuiteBatchSink):
    name = "VendorCredits"
    record_type = "vendorCredit"
    unified_schema = VendorCredit
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        tran_ids = {record["vendorCreditNumber"] for record in raw_records if record.get("vendorCreditNumber")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, vendor_credits = self.suite_talk_client.get_transaction_data(
            transaction_type="VendCred",
            record_ids=ids,
            external_ids=external_ids,
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

        vendor_credit_ids = {vendor_credit['internalId'] for vendor_credit in vendor_credits}
        _, _, vendor_credit_items = self.suite_talk_client.get_vendor_credit_items(
            vendor_credit_ids
        )

        return {
            **self._target.reference_data,
            self.name: vendor_credits,
            "VendorCreditItems": vendor_credit_items,
            "Vendors": vendors,
            "Items": items
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return VendorCreditSchemaMapper(record, self.name, reference_data).to_netsuite()

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
        tax_details = record.get("taxDetails", {}).get("items", [])
        new_items = []
        new_tax_details = []

        for item in items:
            exists = self.check_item_exists(record['internalId'], item, reference_data)
            if exists:
                continue
            
            new_items.append(item)

            if item_tax_details_reference := item.get("taxDetailsReference"):
                item_tax_details = next((tax_detail for tax_detail in tax_details if tax_detail.get("taxDetailsReference", {}).get("id") == item_tax_details_reference), None)
                if item_tax_details:
                    new_tax_details.append(item_tax_details)

        if new_items:
            new_item_payload = {
                "items": new_items
            }
            record["item"] = new_item_payload
        else:
            record = self._omit_key(record, "item")

        expenses = record.get("expense", {}).get("items", [])
        new_expenses = []
        for expense in expenses:
            exists = self.check_expense_exists(record['internalId'], expense, reference_data)
            if exists:
                continue

            new_expenses.append(expense)

            if expense_tax_details_reference := item.get("taxDetailsReference"):
                expense_tax_details = next((tax_detail for tax_detail in tax_details if tax_detail.get("taxDetailsReference", {}).get("id") == expense_tax_details_reference), None)
                if expense_tax_details:
                    new_tax_details.append(expense_tax_details)

        if new_expenses:
            new_expense_payload = {
                "items": new_expenses
            }
            record["expense"] = new_expense_payload
        else:
            record = self._omit_key(record, "expense")
    
        if new_tax_details:
            record["taxDetails"] = { "items": new_tax_details }
            record["taxDetailsOverride"] = True
        else:
            record = self._omit_key(record, "taxDetails")
            record = self._omit_key(record, "taxDetailsOverride")

        return record

    def check_item_exists(self, record_id, item, reference_data):
        existing_items = reference_data["VendorCreditItems"].get(record_id, {}).get("lineItems", [])
        for existing_item in existing_items:
            does_exist = self.compare_item(existing_item, item)
            if does_exist:
                return True
        return False

    def compare_item(self, existing_item, new_item):
        if existing_item.get("memo") == new_item.get("description") and existing_item.get("memo") != None:
            return True
        return False

    def check_expense_exists(self, record_id, expense, reference_data):
        existing_expenses = reference_data["VendorCreditItems"].get(record_id, {}).get("expenses", [])
        for existing_expense in existing_expenses:
            does_exist = self.compare_expense(existing_expense, expense)
            if does_exist:
                return True
        return False

    def compare_expense(self, existing_expense, new_expense):
        if existing_expense.get("memo") == new_expense.get("memo") and existing_expense.get("memo") != None:
            return True
        return False
