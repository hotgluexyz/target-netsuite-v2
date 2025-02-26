from target_netsuite_v2.sinks import NetSuiteBatchSink
# from target_netsuite_v2.mapper.account_schema_mapper import AccountSchemaMapper

class BillSink(NetSuiteBatchSink):
    name = "Bills"
    record_type = "transaction"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        _, _, bills = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        vendor_ids = {record["vendor"] for record in raw_records if record.get("vendor")}
        vendor_ids.update(record["vendorRef"]["id"] for record in raw_records if record.get("vendorRef", {}).get("id"))
        vendor_names = {record["vendorRef"]["name"] for record in raw_records if record.get("vendorRef", {}).get("name")}
        _, _, vendors = self.suite_talk_client.get_reference_data(
            "vendor",
            record_ids=vendor_ids,
            names=vendor_names
        )

        item_ids = {}
        item_names = {}
        for record in raw_records:
            item_ids.update(line_item["item"] for line_item in record.get("lineItems", []) if line_item.get("item"))
            item_ids.update(line_item["itemRef"]["id"] for line_item in record.get("lineItems", []) if line_item.get("itemRef").get("id"))
            item_names.update(line_item["itemRef"]["name"] for line_item in record.get("lineItems", []) if line_item.get("itemRef").get("name"))
        _, _, items = self.suite_talk_client.get_reference_data(
            "item",
            record_ids = item_ids,
            names = item_names
        )

        return {
            **self._target.reference_data,
            "Bills": bills,
            "Vendors": vendors,
            "Items": items
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        # return AccountSchemaMapper(record, self.name, reference_data).to_netsuite()
        return {}
