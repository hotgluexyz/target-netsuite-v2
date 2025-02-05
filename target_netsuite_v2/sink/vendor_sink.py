from target_netsuite_v2.client import NetSuiteSink
from target_netsuite_v2.mapper.vendor_schema_mapper import VendorSchemaMapper


class VendorSink(NetSuiteSink):
    name = "Vendors"
    endpoint = "/vendor"

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("id"))

    def preprocess_record(self, record: dict, context: dict) -> dict:
        vendor = None
        # if record.get("id"):
        #     vendor = list(
        #         filter(
        #             lambda x: x["internalId"] == record.get("id")
        #             or x["externalId"] == record.get("id"),
        #             context["Vendors"],
        #         )
        #     )

        # address = record.get("addresses")
        # phoneNumber = record.get("phoneNumbers")
        # vendor_mapping = {
        #     "email": record.get("emailAddress"),
        #     "companyName": record.get("vendorName"),
        #     "dateCreated": record.get("createdAt"),
        #     "entityId": record.get("vendorName"),
        #     "firstName": record.get("contactName"),
        #     "lastModifiedDate": record.get("updatedAt"),
        #     "currency": {"refName": record.get("currency")},
        #     "homePhone": phoneNumber[0]["number"] if phoneNumber else None,
        #     "defaultAddress": f"{address[0]['line1']} {address[0]['line2']} {address[0]['line3']}, {address[0]['city']}, {address[0]['state'], address[0]['country'], address[0]['postalCode']}"
        #     if address
        #     else None,
        # }

        # if record.get("subsidiary"):
        #     vendor_mapping["subsidiary"] = {"id": record.get("subsidiary")}

        # if vendor:
        #     vendor_mapping["internalId"] = vendor[0].get("internalId")
        #     vendor_mapping["accountNumber"] = vendor[0].get("accountNumber")

        vendor_mapping = VendorSchemaMapper(record, context).to_netsuite()
        return vendor_mapping