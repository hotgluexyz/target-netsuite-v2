from target_netsuite_v2.mapper.base_mapper import BaseMapper
from target_netsuite_v2.mapper.invoice_line_item_schema_mapper import InvoiceLineItemSchemaMapper

class InvoiceSchemaMapper(BaseMapper):
    """A class responsible for mapping an account record ingested in the unified schema format to a payload for NetSuite"""
    field_mappings = {
        "externalId": "externalId",
        "dueDate": "dueDate",
        "issueDate": "tranDate",
        "shipDate": "shipDate",
        "exchangeRate": "exchangeRate",
        "relatedPayments": "relatedPayments"
    }

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        if "subsidiaryId" in self.record or "subsidiaryName" in self.record:
            subsidiary_id = self._find_reference_by_id_or_ref(
                self.reference_data["Subsidiaries"],
                "subsidiaryId",
                "subsidiaryName"
            )["internalId"]
        elif self.existing_record:
            subsidiary_id = self.existing_record["subsidiaryId"]
        else:
            subsidiary_id = None

        payload = {
            **self._map_internal_id(),
            **self._map_entity(),
            **self._map_currency(),
            **self._map_custom_fields(),
            **self._map_subrecord("Locations", "locationId", "locationName", "location", subsidiary_scope=subsidiary_id),
            **self._map_subrecord("Subsidiaries", "subsidiaryId", "subsidiaryName", "subsidiary"),
            **self._map_billing_address(),
            **self._map_shipping_address(),
            **self._map_invoice_line_items(subsidiary_id)
        }

        self._map_fields(payload)

        return payload

    def _map_entity(self):
        reference = self._find_reference_by_id_or_ref(
            self.reference_data["Customers"],
            "customerId",
            "customerName"
        )

        if reference:
            return { "entity": { "id": reference["internalId"] } }

        return {}

    def _map_invoice_line_items(self, subsidiary_id):
        line_items = self.record.get("lineItems", [])
        mapped_line_items = []

        for line_item in line_items:
            payload = InvoiceLineItemSchemaMapper(line_item, self.reference_data, subsidiary_id).to_netsuite()
            mapped_line_items.append(payload)

        if mapped_line_items:
            return { "item": { "items": mapped_line_items } }
        else:
            return {}

    def _map_billing_address(self):
        addresses = self.record.get("addresses", [])
        for address in addresses:
            if address.get("addressType") == "billing":
                return {
                    "billingAddress": {
                        "addrText": address.get("addressText"),
                        "addr1": address.get("line1"),
                        "addr2": address.get("line2"),
                        "addr3": address.get("line3"),
                        "city": address.get("city"),
                        "state": address.get("state"),
                        "country": address.get("country"),
                        "zip": address.get("postalCode")
                    }
                }
        return {}

    def _map_shipping_address(self):
        addresses = self.record.get("addresses", [])
        for address in addresses:
            if address.get("addressType") == "shipping":
                return {
                    "shippingAddress": {
                        "addrText": address.get("addressText"),
                        "addr1": address.get("line1"),
                        "addr2": address.get("line2"),
                        "addr3": address.get("line3"),
                        "city": address.get("city"),
                        "state": address.get("state"),
                        "country": address.get("country"),
                        "zip": address.get("postalCode")
                    }
                }
        return {}
