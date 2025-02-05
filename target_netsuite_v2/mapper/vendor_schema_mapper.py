class VendorSchemaMapper:
    """A class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    PHONE_TYPE_MAP = { "unknown": "phone", "mobile": "mobilePhone", "home": "homePhone" }
    ADDRESS_TYPE_MAP = { "shipping": "defaultShippingAddress", "billing": "defaultBillingAddress" }

    def __init__(
            self,
            record,
            context
    ) -> None:
        self.record = record
        self.context = context

    def _map_currency(self):
        """Extracts a currency object in NetSuite format"""
        currency_symbol = self.record.get("currency")
        currency = next(
            (item for item in self.context["Currencies"] if item["symbol"] == currency_symbol),
            None
        )
        if currency:
            return { "currency": { "refName": currency["name"] } }
        else:
            return {}

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
        address_map = {"shipping": "defaultShippingAddress", "billing": "defaultBillingAddress"}
        addresses = {}

        for addr in self.record.get("addresses", []):
            ns_key = address_map.get(addr.get("addressType"))
            if ns_key:
                addresses[ns_key] = f"{addr.get('line1', '')} {addr.get('line2', '')} {addr.get('line3', '')}, {addr.get('city', '')}, {addr.get('state', '')}, {addr.get('country', '')}, {addr.get('postalCode', '')}".replace("  ", " ").strip()

        return addresses

    def to_netsuite(self) -> dict:
        """Transforms the unified record into a NetSuite-compatible payload."""
        return {
            "id": self.record.get("id"),
            "externalId": self.record.get("externalId"),
            "companyName": self.record.get("vendorName"),
            "salutation": self.record.get("prefix"),
            "firstName": self.record.get("firstName"),
            "middleName": self.record.get("middleName"),
            "lastName": self.record.get("lastName"),
            "title": self.record.get("title"),
            "email": self.record.get("email"),
            "url": self.record.get("website"),
            "printOnCheckAs": self.record.get("checkName"),
            "balance": self.record.get("balance"),
            "isInactive": not self.record.get("isActive", True),
            "lastModifiedDate": self.record.get("updatedAt"),
            "dateCreated": self.record.get("createdAt"),
            **self._map_phone_numbers(),
            **self._map_addresses(),
            **self._map_currency()
        }