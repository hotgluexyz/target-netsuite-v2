from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        return {
            **self._target.reference_data,
            **self.get_primary_records_for_batch(context)
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, reference_data).to_netsuite()
