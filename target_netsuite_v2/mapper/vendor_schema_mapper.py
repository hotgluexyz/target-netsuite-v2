from target_netsuite_v2.mapper.base_mapper import BaseMapper

class VendorSchemaMapper(BaseMapper):
    """A class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    PHONE_TYPE_MAP = { "unknown": "phone", "mobile": "mobilePhone", "home": "homePhone" }
    ADDRESS_TYPE_MAP = { "shipping": "defaultShippingAddress", "billing": "defaultBillingAddress" }

    def _map_phone_numbers(self):
        """Extracts phone numbers in NetSuite format."""
        phones = {}

        for pn in self.record.get("phoneNumbers", []):
            phone_type = self.PHONE_TYPE_MAP.get(pn.get("type"))
            if phone_type:
                phones[phone_type] = pn["phoneNumber"]

        return phones

    def _map_addresses(self):
        """Extracts addresses in NetSuite format."""
        addresses = {}

        for addr in self.record.get("addresses", []):
            ns_key = self.ADDRESS_TYPE_MAP.get(addr.get("addressType"))
            if ns_key:
                addresses[ns_key] = f"{addr.get('line1', '')} {addr.get('line2', '')} {addr.get('line3', '')}, {addr.get('city', '')}, {addr.get('state', '')}, {addr.get('country', '')}, {addr.get('postalCode', '')}".replace("  ", " ").strip()

        return addresses

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        payload = {
            **self._map_internal_id("Vendors"),
            **self._map_phone_numbers(),
            **self._map_addresses(),
            **self._map_currency(),
            **self._map_subrecord("Subsidiaries", "subsidiary", "subsidiaryRef")
        }

        field_mappings = {
            "externalId": "externalId",
            "vendorName": "companyName",
            "prefix": "salutation",
            "firstName": "firstName",
            "middleName": "middleName",
            "lastName": "lastName",
            "title": "title",
            "email": "email",
            "website": "url",
            "checkName": "printOnCheckAs",
            "balance": "balance",
            "updatedAt": "lastModifiedDate",
            "createdAt": "dateCreated"
        }

        # Only add fields that exist in the record
        for record_key, payload_key in field_mappings.items():
            if record_key in self.record:
                payload[payload_key] = self.record.get(record_key)

        # Handle isInactive separately due to its inverse logic
        if "isActive" in self.record:
            payload["isInactive"] = not self.record.get("isActive", True)

        return payload