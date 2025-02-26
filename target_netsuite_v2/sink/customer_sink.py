from target_netsuite_v2.sinks import NetSuiteBatchSink
from target_netsuite_v2.mapper.customer_schema_mapper import CustomerSchemaMapper

class CustomerSink(NetSuiteBatchSink):
    name = "Customers"
    record_type = "customer"

    def get_batch_reference_data(self, context) -> dict:
        raw_records = context["records"]

        # Get customer ids
        ids = {record["id"] for record in raw_records if record.get("id")}
        ids.update(record["parent"] for record in raw_records if record.get("parent"))
        ids.update(record["parentRef"]["id"] for record in raw_records if record.get("parentRef", {}).get("id"))

        # Get customer external ids
        external_ids = {record["externalId"] for record in raw_records if record.get("externalId")}

        # Get customer names
        names = {record["companyName"] for record in raw_records if record.get("companyName")}
        names.update(record["parentRef"]["name"] for record in raw_records if record.get("parentRef", {}).get("name"))

        # Get customers
        _, _, customers = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids,
            names=names
        )

        # Get sales rep ids
        sales_rep_ids = {record["salesRep"] for record in raw_records if record.get("salesRep")}
        sales_rep_ids.update(record["salesRepRef"]["id"] for record in raw_records if record.get("salesRepRef", {}).get("id"))

        # Get sales rep names
        sales_rep_names = {record["salesRepRef"]["name"] for record in raw_records if record.get("salesRepRef", {}).get("name")}

        _, _, employees = self.suite_talk_client.get_reference_data(
            "employee",
            record_ids=sales_rep_ids,
            names=sales_rep_names
        )

        _, _, addresses = self.suite_talk_client.get_default_addresses(self.record_type, ids)

        return {
            **self._target.reference_data,
            self.name: customers,
            "Addresses": addresses,
            "Employees": employees
        }

    def preprocess_batch_record(self, record: dict, reference_data: dict) -> dict:
        return CustomerSchemaMapper(record, self.name, reference_data).to_netsuite()
