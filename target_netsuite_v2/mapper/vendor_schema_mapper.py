class VendorSchemaMapper:
    """A class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    PHONE_TYPE_MAP = { "unknown": "phone", "mobile": "mobilePhone", "home": "homePhone" }
    ADDRESS_TYPE_MAP = { "shipping": "defaultShippingAddress", "billing": "defaultBillingAddress" }

    def __init__(
            self,
            record,
            reference_data
    ) -> None:
        self.record = record
        self.reference_data = reference_data

    def _find_existing_vendor(self):
        """Finds an existing vendor by matching internal or external ID."""
        if not self.record.get("id"):
            return None

        return next(
            (vendor for vendor in self.reference_data["Vendors"]
             if vendor["internalId"] == self.record["id"]
             or vendor["externalId"] == self.record["id"]),
            None
        )

    def _map_subsidiary(self):
        """Extracts a subsidiary object in NetSuite format"""
        subsidiary_id = self.record.get("subsidiary", {}).get("id")
        subsidiary = next(
            (item for item in self.reference_data["Subsidiaries"] if item["internalId"] == subsidiary_id),
            None
        )
        if subsidiary:
            return { "subsidiary": { "id": subsidiary["internalId"] } }
        else:
            return {}

    def _map_currency(self):
        """Extracts a currency object in NetSuite format"""
        currency_symbol = self.record.get("currency")
        currency = next(
            (item for item in self.reference_data["Currencies"] if item["symbol"] == currency_symbol),
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
        addresses = {}

        for addr in self.record.get("addresses", []):
            ns_key = self.ADDRESS_TYPE_MAP.get(addr.get("addressType"))
            if ns_key:
                addresses[ns_key] = f"{addr.get('line1', '')} {addr.get('line2', '')} {addr.get('line3', '')}, {addr.get('city', '')}, {addr.get('state', '')}, {addr.get('country', '')}, {addr.get('postalCode', '')}".replace("  ", " ").strip()

        return addresses

    def _map_internal_id(self):
        vendor = self._find_existing_vendor()
        if vendor:
            return { "internalId": vendor["internalId"]}
        else:
            return {}

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
            **self._map_currency(),
            **self._map_subsidiary(),
            **self._map_internal_id()
        }