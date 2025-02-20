from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = set()
        external_ids = set()

        for record in raw_records:
            if record.get("id"):
                ids.add(record["id"])

            if record.get("parent"):
                ids.add(record["parent"])

            if record.get("parentRef", {}).get("id"):
                ids.add(record["parentRef"]["id"])

            if record.get("externalId"):
                external_ids.add(record["externalId"])

        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, ids)

        return {
            **self._target.reference_data,
            self.name: items,
            "Addresses": addresses
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, reference_data).to_netsuite()
