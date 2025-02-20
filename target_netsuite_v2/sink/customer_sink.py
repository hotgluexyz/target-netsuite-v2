from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = set()
        external_ids = set()
        sales_rep_ids = set()

        for record in raw_records:
            if record.get("id"):
                ids.add(record["id"])

            if record.get("parent"):
                ids.add(record["parent"])

            if record.get("parentRef", {}).get("id"):
                ids.add(record["parentRef"]["id"])

            if record.get("externalId"):
                external_ids.add(record["externalId"])

            if record.get("salesRep"):
                sales_rep_ids.add(record["salesRep"])

            if record.get("salesRepRef", {}).get("id"):
                sales_rep_ids.add(record["salesRepRef"]["id"])

        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        _, _, employees = self.suite_talk_client.get_reference_data(
            "employee",
            record_ids=sales_rep_ids,
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, ids)

        return {
            **self._target.reference_data,
            self.name: items,
            "Addresses": addresses,
            "Employees": employees
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, self.name, reference_data).to_netsuite()
