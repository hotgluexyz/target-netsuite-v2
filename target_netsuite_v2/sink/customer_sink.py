from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = []
        external_ids = []

        for record in raw_records:
            if record.get("id"):
                ids.append(record["id"])

            if record.get("externalId"):
                external_ids.append(record["externalId"])

        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        batch_specific_data = { self.name: items }

        return {
            **self._target.reference_data,
            **batch_specific_data
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, reference_data).to_netsuite()
