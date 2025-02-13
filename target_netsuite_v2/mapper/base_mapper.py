class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""

    def __init__(
            self,
            record,
            reference_data
    ) -> None:
        self.record = record
        self.reference_data = reference_data

    def _find_existing_record(self, reference_list):
        """Finds an existing record by matching internal or external ID.
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

    def _find_reference_by_id_or_ref(self, reference_list, main_field, ref_field):
        """Generic method to find a reference either by direct ID or through a reference object
        Args:
            reference_list (list): List of reference data to search through (e.g. Accounts, Locations)
            main_field (str): Name of the direct ID field (e.g. "parent", "location")
            ref_field (str): Name of the reference object field (e.g. "parentRef", "locationRef")

        Returns:
            dict|None: Matching reference object or None if not found
        """
        # Check for direct ID field first
        if direct_id := self.record.get(main_field):
            return next(
                (item for item in reference_list if item["internalId"] == direct_id),
                None
            )

        # If no direct ID field, check for reference object
        ref_obj = self.record.get(ref_field)
        if not ref_obj:
            return None

        # Try to find by reference object's ID first
        if ref_id := ref_obj.get("id"):
            found_item = next(
                (item for item in reference_list if item["internalId"] == ref_id),
                None
            )
            if found_item:
                return found_item

        # If no match by id, try to find by reference object's name
        if ref_name := ref_obj.get("name"):
            return next(
                (item for item in reference_list if item["name"] == ref_name),
                None
            )

        return None

    def _find_references_by_id_or_ref(self, reference_list, main_field, ref_field):
        """Generic method to find multiple references either by direct IDs or through reference objects
        Args:
            reference_list (list): List of reference data to search through (e.g. Subsidiaries)
            main_field (str): Name of the direct ID field (e.g. "subsidiary")
            ref_field (str): Name of the reference object field (e.g. "subsidiaryRef")

        Returns:
            list[dict]: List of matching reference objects. Empty list if none found.
        """
        matches = set()  # Use set to avoid duplicates

        # Check for direct ID array first
        direct_ids = self.record.get(main_field, [])
        for direct_id in direct_ids:
            if found := next(
                (item for item in reference_list if item["internalId"] == direct_id),
                None
            ):
                matches.add(found["internalId"])

        # Check for reference objects array
        ref_objects = self.record.get(ref_field, [])
        for ref_obj in ref_objects:
            # Try to find by reference object's ID first
            if ref_id := ref_obj.get("id"):
                if found := next(
                    (item for item in reference_list if item["internalId"] == ref_id),
                    None
                ):
                    matches.add(found["internalId"])
                    continue

            # If no match by id, try to find by reference object's name
            if ref_name := ref_obj.get("name"):
                if found := next(
                    (item for item in reference_list if item["name"] == ref_name),
                    None
                ):
                    matches.add(found["internalId"])

        # Convert the set of internalIds back to full reference objects
        return [
            item for item in reference_list
            if item["internalId"] in matches
        ]

    def _find_existing_currency(self, currency_symbol):
        """Find a currency in the reference data by searching by symbol"""
        return next(
            (item for item in self.reference_data["Currencies"] if item["symbol"] == currency_symbol),
            None
        )

    def _map_subrecord(self, reference_type, field_name, ref_field_name):
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
            field_name,
            ref_field_name
        )

        if reference:
            return { field_name: { "id": reference["internalId"] } }

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

    def _map_internal_id(self, reference_type):
        record = self._find_existing_record(self.reference_data[reference_type])
        if record:
            return { "internalId": record["internalId"]}
        return {}

    def _map_currency(self):
        """Extracts a currency object in NetSuite format"""
        currency = self._find_existing_currency(self.record.get("currency"))
        if currency:
            return { "currency": { "id": currency["internalId"] } }

        return {}