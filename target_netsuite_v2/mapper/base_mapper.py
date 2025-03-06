class InvalidInputError(Exception):
    pass

class InvalidReferenceError(InvalidInputError):
    pass

class InvalidAddressError(InvalidInputError):
    pass

class InvalidPhoneError(InvalidInputError):
    pass

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    PHONE_TYPE_MAP = { "unknown": "phone", "mobile": "mobilePhone", "home": "homePhone" }
    ADDRESS_TYPE_MAP = {
        "shipping": { "defaultShipping": True, "defaultBilling": False },
        "billing": { "defaultShipping": False, "defaultBilling": True }
    }

    def __init__(
            self,
            record,
            sink_name,
            reference_data
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data = reference_data
        self.existing_record = self._find_existing_record(self.reference_data[sink_name])

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal or external ID.
            1. If the ingested record has an "id" field, we first look for a record in the reference data whose "internalId" matches the record "id"
            2. If the ingested record has an "id" field, and no record was found in the first step, we look for a record in the reference data whose "externalId" matches the record "id"
            3. If the ingested record has an "externalId" field, and no "id" was provided, we look for a record in the reference data whose "externalId" matches the record "externalId"
        """

        if record_id := self.record.get("id"):
            # Try matching internal ID first
            found_record = next(
                (record for record in reference_list
                if record["internalId"] == record_id),
                None
            )
            if found_record:
                return found_record

            # Try matching external ID if internal ID match failed
            return next(
                (record for record in reference_list
                if record.get("externalId") == record_id),
                None
            )

        # If no ID provided, try matching by external ID
        if external_id := self.record.get("externalId"):
            return next(
                (record for record in reference_list
                if record.get("externalId") == external_id),
                None
            )

        return None

    def _find_subsidiaries(self, main_field, ref_field):
        reference_list = self.reference_data["Subsidiaries"]

        matches = set()
        missing_references = []

        direct_ids = self.record.get(main_field, [])
        for direct_id in direct_ids:
            found = next((item for item in reference_list if item["internalId"] == direct_id), None)
            if found:
                matches.add(found["internalId"])
            else:
                missing_references.append(f"ID {direct_id}")

        ref_objects = self.record.get(ref_field, [])
        for ref_obj in ref_objects:
            ref_id = ref_obj.get("id")
            ref_name = ref_obj.get("name")
            found = None

            if ref_id:
                found = next((item for item in reference_list if item["internalId"] == ref_id), None)
            if found:
                matches.add(found["internalId"])
            elif ref_name:
                found = next((item for item in reference_list if item.get("name") == ref_name), None)
                if found:
                    matches.add(found["internalId"])

            if not found:
                missing_info = f"ID {ref_id}" if ref_id else f"Name {ref_name}"
                missing_references.append(missing_info)

        # If any references were missing, raise an error
        if missing_references:
            error_message = f"Unable to find references for {main_field}. " + "Missing: " + ", ".join(missing_references)
            raise InvalidReferenceError(error_message)

        return [item for item in reference_list if item["internalId"] in matches]

    def _find_reference_by_id_or_ref(self, reference_list, id_field, name_field, subsidiary_scope=None):
        """Generic method to find a reference either by direct ID or through a reference object
        Args:
            reference_list (list): List of reference data to search through (e.g. Accounts, Locations)
            id_field (str): Name of the direct ID field
            name_field (str): Name of the reference name field
            subsidiary_scope (str): The ID of a subsidiary to filter by when doing name search

        Returns:
            dict|None: Matching reference object or None if not found
        """
        found = None
        # Check for direct ID field first
        if direct_id := self.record.get(id_field):
            found = next(
                (item for item in reference_list if item["internalId"] == direct_id),
                None
            )

        if found:
            return found

        # If no match by id, try to find by reference name and subsidiary scope if provided.
        if ref_name := self.record.get(name_field):
            found = next(
                (
                    item
                    for item in reference_list
                    if item.get("name") == ref_name and
                    (subsidiary_scope is None or item.get("subsidiaryId") == subsidiary_scope)
                ),
                None
            )

        if found:
            return found

        # Raise an `InvalidReferenceError` if either the id or the name was provided for a reference field, but it was not found
        if direct_id or ref_name:
            lookup_attempts = []
            if direct_id:
                lookup_attempts.append(f"by id {direct_id}")
            if ref_name:
                lookup_attempts.append(f"by name {ref_name}")
            if subsidiary_scope:
                lookup_attempts.append(f"within subsidiary {subsidiary_scope}")

            # Properly format with "and" before the last item
            if len(lookup_attempts) > 1:
                lookup_message = ", ".join(lookup_attempts[:-1]) + f", and {lookup_attempts[-1]}"
            else:
                lookup_message = lookup_attempts[0]

            error_message = f"Unable to find {id_field.replace('Id', '')}. Tried lookup {lookup_message}."

            raise InvalidReferenceError(error_message)

    def _find_references_by_id_or_ref(self, reference_list, main_field, ref_field):
        """Generic method to find multiple references either by direct IDs or through reference objects.

        Args:
            reference_list (list): List of reference data to search through (e.g. Subsidiaries)
            main_field (str): Name of the direct ID field (e.g. "subsidiary")
            ref_field (str): Name of the reference object field (e.g. "subsidiaryRef")

        Returns:
            list[dict]: List of matching reference objects. Raises an error if any reference is not found.
        """
        matches = set()
        missing_references = []

        direct_ids = self.record.get(main_field, [])
        for direct_id in direct_ids:
            found = next((item for item in reference_list if item["internalId"] == direct_id), None)
            if found:
                matches.add(found["internalId"])
            else:
                missing_references.append(f"ID {direct_id}")

        ref_objects = self.record.get(ref_field, [])
        for ref_obj in ref_objects:
            ref_id = ref_obj.get("id")
            ref_name = ref_obj.get("name")
            found = None

            if ref_id:
                found = next((item for item in reference_list if item["internalId"] == ref_id), None)
            if found:
                matches.add(found["internalId"])
            elif ref_name:
                found = next((item for item in reference_list if item.get("name") == ref_name), None)
                if found:
                    matches.add(found["internalId"])

            if not found:
                missing_info = f"ID {ref_id}" if ref_id else f"Name {ref_name}"
                missing_references.append(missing_info)

        # If any references were missing, raise an error
        if missing_references:
            error_message = f"Unable to find references for {main_field}. " + "Missing: " + ", ".join(missing_references)
            raise InvalidReferenceError(error_message)

        return [item for item in reference_list if item["internalId"] in matches]

    def _find_existing_currency(self):
        """Find a currency in the reference data by searching by symbol, name, or ID."""
        reference_list = self.reference_data["Currencies"]

        found = None
        # Check for direct ID field first
        if direct_id := self.record.get("currencyId"):
            found = next(
                (item for item in reference_list if item["internalId"] == direct_id),
                None
            )

        if found:
            return found

        # If no match by id, try to find by name
        if ref_name := self.record.get("currencyName"):
            found = next(
                (item for item in reference_list if item["name"] == ref_name),
                None
            )

        if found:
            return found

        # If no match by id, or name try to find by symbol
        if ref_symbol := self.record.get("currency"):
            found = next(
                (item for item in reference_list if item["symbol"] == ref_symbol),
                None
            )

        if found:
            return found

        # Raise an `InvalidReferenceError` if either the id or the name was provided for a reference field, but it was not found
        if direct_id or ref_name or ref_symbol:
            lookup_attempts = []
            if direct_id:
                lookup_attempts.append(f"by id {direct_id}")
            if ref_name:
                lookup_attempts.append(f"by name {ref_name}")
            if ref_symbol:
                lookup_attempts.append(f"by symbol {ref_symbol}")

            # Properly format with "and" before the last item
            if len(lookup_attempts) > 1:
                lookup_message = ", ".join(lookup_attempts[:-1]) + f", and {lookup_attempts[-1]}"
            else:
                lookup_message = lookup_attempts[0]

            error_message = f"Unable to find currency. Tried lookup {lookup_message}."

            raise InvalidReferenceError(error_message)

    def _map_subrecord(self, reference_type, id_field, name_field, target_field, subsidiary_scope=None):
        """Generic method to map a subrecord reference to NetSuite format
        Args:
            reference_type (str): Key in reference_data (e.g. "Accounts", "Locations")
            field_name (str): Base name of the field (e.g. "parent", "location")
            ref_field_name (str): Name of the reference field (e.g. "parentRef", "locationRef")

        Returns:
            dict: Mapped subrecord reference or empty dict if not found
        """
        reference = self._find_reference_by_id_or_ref(
            self.reference_data[reference_type],
            id_field,
            name_field,
            subsidiary_scope
        )

        if reference:
            return { target_field: { "id": reference["internalId"] } }

        return {}

    def _map_subrecord_list(self, reference_type, field_name, ref_field_name):
        """Generic method to map a list of subrecord references to NetSuite format
        Args:
            reference_type (str): Key in reference_data (e.g. "Subsidiaries")
            field_name (str): Base name of the field (e.g. "subsidiary")
            ref_field_name (str): Name of the reference field (e.g. "subsidiaryRef")

        Returns:
            dict: Mapped list of subrecord references or empty dict if none found
        """
        references = self._find_references_by_id_or_ref(
            self.reference_data[reference_type],
            field_name,
            ref_field_name
        )
        if references:
            return {
                field_name: {
                    "items": [{"id": ref["internalId"]} for ref in references]
                }
            }
        return {}

    def _map_internal_id(self):
        if self.existing_record:
            return { "internalId": self.existing_record["internalId"]}
        return {}

    def _map_currency(self):
        """Extracts a currency object in NetSuite format"""
        currency = self._find_existing_currency()
        if currency:
            return { "currency": { "id": currency["internalId"] } }

        return {}

    def _map_phone_numbers(self):
        """Extracts phone numbers in NetSuite format."""
        phones = {}

        for pn in self.record.get("phoneNumbers", []):
            phone_type = self.PHONE_TYPE_MAP.get(pn.get("type"))
            if phone_type:
                phones[phone_type] = pn.get("phoneNumber")
            else:
                raise InvalidPhoneError(f"Unsupported phone type: {phone_type}")

        return phones

    def _map_addressbookaddress(self, unified):
        return {
            **self.ADDRESS_TYPE_MAP[unified["addressType"]],
            "addressbookaddress": {
                "addrText": unified.get("addressText"),
                "addr1": unified.get("line1"),
                "addr2": unified.get("line2"),
                "addr3": unified.get("line3"),
                "city": unified.get("city"),
                "state": unified.get("state"),
                "country": unified.get("country"),
                "zip": unified.get("postalCode")
            }
        }

    def _check_for_existing_address(self, unified, address_type, record_id):
        """Checks if an address already exists in the reference data."""
        existing_address = self.reference_data["Addresses"].get(record_id, {}).get(address_type)
        if not existing_address:
            return False

        fields_to_compare = {
            "addrtext": "addressText",
            "addr1": "line1",
            "addr2": "line2",
            "addr3": "line3",
            "city": "city",
            "state": "state",
            "country": "country",
            "zip": "postalCode"
        }

        return all(
            unified.get(unified_field) == existing_address.get(existing_field)
            for existing_field, unified_field in fields_to_compare.items()
        )

    def _map_addressbook(self):
        """Extracts addresses to a NetSuite addressbook."""
        in_addresses = self.record.get("addresses", [])
        if not in_addresses:
            return {}

        out_addresses = []
        for addr in in_addresses:
            if addr.get("addressType") in self.ADDRESS_TYPE_MAP:
                if not self.existing_record or not self._check_for_existing_address(addr, addr["addressType"], self.existing_record["internalId"]):
                    out_addresses.append(self._map_addressbookaddress(addr))
            else:
                raise InvalidAddressError(f"Unsupported addressType: {addr.get('addressType')}")

        return { "addressbook": { "items": out_addresses } } if out_addresses else {}

    def _map_custom_fields(self):
        """Maps custom fields to a dictionary of name-value pairs, excluding None values."""
        custom_fields = self.record.get("customFields", [])
        return {field["name"]: field["value"] for field in custom_fields if field["value"] is not None}
