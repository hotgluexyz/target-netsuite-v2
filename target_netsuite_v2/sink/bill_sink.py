from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.bill_schema_mapper import BillSchemaMapper
from target_netsuite_v2.mapper.bill_payment_schema_mapper import BillPaymentSchemaMapper
from target_netsuite_v2.mapper.base_mapper import InvalidInputError

class BillSink(NetSuiteBatchSink):
    name = "Bills"
    record_type = "vendorBill"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        tran_ids = {record["billNumber"] for record in raw_records if record.get("billNumber")}
        ids = {record["id"] for record in raw_records if record.get("id")}
        _, _, bills = self.suite_talk_client.get_transaction_data(
            transaction_type="VendBill",
            external_ids=external_ids,
            record_ids=ids,
            tran_ids=tran_ids
        )

        vendor_ids = {record["vendorId"] for record in raw_records if record.get("vendorId")}
        vendor_entity_ids = {record["vendorNumber"] for record in raw_records if record.get("vendorNumber")}
        vendor_names = {record["vendorName"] for record in raw_records if record.get("vendorName")}
        vendor_external_ids = {record["vendorExternalId"] for record in raw_records if record.get("vendorExternalId")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            names=vendor_names,
            external_ids=vendor_external_ids,
            entity_ids=vendor_entity_ids
        )

        item_record_ids = set()
        item_ids = set()
        item_names = set()
        for record in raw_records:
            item_record_ids.update(line_item["itemId"] for line_item in record.get("lineItems", []) if line_item.get("itemId"))
            item_ids.update(line_item["itemNumber"] for line_item in record.get("lineItems", []) if line_item.get("itemNumber"))
            item_names.update(line_item["itemName"] for line_item in record.get("lineItems", []) if line_item.get("itemName"))
        _, _, items = self.suite_talk_client.get_reference_data(
            "item",
            record_ids = item_record_ids,
            names = item_names,
            item_ids=item_ids
        )

        bill_ids = {bill["internalId"] for bill in bills}
        _, _, bill_items = self.suite_talk_client.get_bill_items(
            bill_ids
        )
        
        _, _, bill_payments = self.suite_talk_client.get_bill_payments(
            bill_ids=bill_ids
        )

        return {
            **self._target.reference_data,
            "Bills": bills,
            "Vendors": vendors,
            "Items": items,
            "BillItems": bill_items,
            "BillPayments": bill_payments
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return BillSchemaMapper(record, self.name, reference_data).to_netsuite()

    def upsert_record(self, record: dict, reference_data: dict):
        state = {}

        _record = self._omit_key(record, "relatedPayments")

        if self.record_exists(record):
            post_processed_record = self.post_processing_for_update(_record, reference_data)
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, _record['internalId'], post_processed_record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            _, success, error_messages = self.create_child_records(id, record, reference_data)

            if error_messages:
                state["error"] = error_messages
            else:
                state["is_updated"] = True
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, _record)

            if error_message:
                state["error"] = error_message
                return id, success, state

            _, success, error_messages = self.create_child_records(id, record, reference_data)

            if error_messages:
                state["error"] = error_messages

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

        expenses = record.get("expense", {}).get("items", [])
        new_expenses = []
        for expense in expenses:
            exists = self.check_expense_exists(record['internalId'], expense, reference_data)
            if not exists:
                new_expenses.append(expense)

        if new_expenses:
            new_expense_payload = {
                "items": new_expenses
            }
            record["expense"] = new_expense_payload
        else:
            record = self._omit_key(record, "expense")

        return record

    def check_item_exists(self, record_id, item, reference_data):
        existing_items = reference_data["BillItems"].get(record_id, {}).get("lineItems", [])
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
        existing_expenses = reference_data["BillItems"].get(record_id, {}).get("expenses", [])
        for existing_expense in existing_expenses:
            does_exist = self.compare_expense(existing_expense, expense)
            if does_exist:
                return True
        return False

    def compare_expense(self, existing_expense, new_expense):
        if existing_expense.get("memo") == new_expense.get("memo") and existing_expense.get("memo") != None:
            return True
        return False

    def create_child_records(self, parent_id: int, record: dict, reference_data: dict):
        payments = record.get("relatedPayments", [])

        created_ids = []
        error_messages = []
        for payment in payments:
            if self.check_payment_exists(parent_id, payment, reference_data):
                continue

            try:
                preprocessed_payment = BillPaymentSchemaMapper(payment, "BillPayments", record.get("entity"), parent_id, reference_data).to_netsuite()
                id, success, error_message = self.suite_talk_client.create_record("vendorPayment", preprocessed_payment)
                if not success:
                    error_messages.append(f"Error creating payment for Bill: {error_message}")
                else:
                    created_ids.append(id)
            except InvalidInputError as e:
                error_messages.append(f"Error creating payment for Bill: {str(e)}")


        return created_ids, len(error_messages) == 0, error_messages

    def check_payment_exists(self, record_id, payment, reference_data):
        existing_payments = reference_data["BillPayments"].get(record_id, {}).get("payments", [])
        for existing_payment in existing_payments:
            does_exist = self.compare_payment(existing_payment, payment)
            if does_exist:
                return True
        return False

    def compare_payment(self, existing_payment, new_payment):
        existing_amount = -(float(existing_payment.get("amount")))
        new_amount = new_payment.get("amount")

        if existing_amount != new_amount:
            return False

        existing_payment_date = existing_payment.get("trandate")
        new_payment_date = new_payment.get("paymentDate")

        if not self._are_dates_equivalent(existing_payment_date, new_payment_date):
            return False

        return True
