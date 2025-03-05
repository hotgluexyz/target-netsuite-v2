from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        ids = {record["id"] for record in raw_records if record.get("id")}
        ids.update(record["parentId"] for record in raw_records if record.get("parentId"))
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}
        names = {record["parentName"] for record in raw_records if record.get("parentName")}
        names.update({record["companyName"] for record in raw_records if record.get("companyName")})

        _, _, customers = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids,
            names=names
        )

        sales_rep_ids = {record["salesRepId"] for record in raw_records if record.get("salesRepId")}
        sales_rep_names = {record["salesRepName"] for record in raw_records if record.get("salesRepName")}
        _, _, employees = self.suite_talk_client.get_reference_data(
            "employee",
            record_ids=sales_rep_ids,
            names=sales_rep_names
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, {customer["internalId"] for customer in customers})

        return {
            **self._target.reference_data,
            self.name: customers,
            "Addresses": addresses,
            "Employees": employees
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, self.name, reference_data).to_netsuite()
