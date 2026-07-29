"""Lazy-loading store for NetSuite reference data tables."""

from netsuitesdk.internal.exceptions import NetSuiteRequestError


class ReferenceDataStore:
    """Loads reference tables on first access instead of bulk-fetching at sink init."""

    def __init__(self, sink):
        self._sink = sink
        self._data = {}
        self._loading = set()

    def get(self, key, default=None):
        self._ensure_loaded(key)
        return self._data.get(key, default)

    def __getitem__(self, key):
        self._ensure_loaded(key)
        if key not in self._data:
            raise KeyError(key)
        return self._data[key]

    def __contains__(self, key):
        return key in self._data

    def _ensure_loaded(self, key):
        if key in self._data or key in self._loading:
            return
        self._loading.add(key)
        try:
            self._sink.logger.info(f"Loading reference data for {key}...")
            rows = self._fetch(key)
            if rows is not None:
                self._data[key] = rows
        finally:
            self._loading.discard(key)

    def _fetch(self, key):
        sink = self._sink
        loaders = {
            "Classifications": self._load_classifications,
            "Currencies": self._load_currencies,
            "Departments": self._load_departments,
            "Accounts": self._load_accounts,
            "Locations": self._load_locations,
            "Customers": self._load_customers,
            "Jobs": self._load_jobs,
            "Vendors": self._load_vendors,
            "Subsidiaries": self._load_subsidiaries,
        }
        loader = loaders.get(key)
        if loader is None:
            sink.logger.warning(f"No reference data loader for key '{key}'")
            return None
        return loader()

    def _load_classifications(self):
        try:
            return self._sink.ns_client.entities["Classifications"].get_all(["name"])
        except Exception as e:
            self._sink._check_exception(e, "Classifications")
            return None

    def _load_currencies(self):
        try:
            return self._sink.ns_client.entities["Currencies"].get_all()
        except Exception as e:
            self._sink._check_exception(e, "Currencies")
            return None

    def _load_departments(self):
        try:
            return self._sink.ns_client.entities["Departments"].get_all(["name"])
        except Exception as e:
            self._sink._check_exception(e, "Departments")
            return None

    def _load_accounts(self):
        try:
            return self._sink.ns_client.entities["Accounts"](
                self._sink.ns_client.ns_client
            ).get_all(
                [
                    "acctName",
                    "acctNumber",
                    "subsidiaryList",
                    "acctType",
                    "class",
                    "department",
                    "isInactive",
                    "location",
                ],
                page_size=100,
            )
        except Exception as e:
            if "You need  the 'Lists -> Documents and Files' permission" in str(e):
                self._sink.logger.info(
                    "Permissions for Documents and Files missing. "
                    "Attempting to get Accounts with body_fields_only=True"
                )
                accounts = self._sink.ns_client.entities["Accounts"](
                    self._sink.ns_client.ns_client, body_fields_only=True
                ).get_all(
                    [
                        "acctName",
                        "acctNumber",
                        "subsidiaryList",
                        "acctType",
                        "class",
                        "department",
                        "isInactive",
                        "location",
                    ],
                    page_size=100,
                )
                self._sink.ns_client.ns_client._search_preferences.bodyFieldsOnly = False
                return accounts
            self._sink._check_exception(e, "Accounts")
            return None

    def _load_locations(self):
        try:
            self._sink.ns_client.ns_client._search_preferences.bodyFieldsOnly = False
            locations = self._sink.ns_client.entities["Locations"].get_all(
                ["name", "subsidiaryList", "isInactive"], page_size=100
            )
            self._sink.logger.info(f"Locations: {locations}")
            return locations
        except NetSuiteRequestError as e:
            message = e.message.replace("error", "failure").replace("Error", "")
            self._sink.logger.warning(
                f"It was not possible to retrieve Locations data: {message}"
            )
            return None
        except Exception as e:
            self._sink._check_exception(e, "Locations")
            return None

    def _load_customers(self):
        try:
            return self._sink.ns_client.entities["Customer"](
                self._sink.ns_client.ns_client
            ).get_all(["companyName", "isInactive", "subsidiary"], page_size=100)
        except Exception as e:
            self._sink._check_exception(e, "Customers")
            return None

    def _load_jobs(self):
        try:
            self._sink.logger.info("Fetching Jobs via REST SuiteQL...")
            return self._sink.get_reference_jobs_rest()
        except Exception as e:
            self._sink._check_exception(e, "Jobs")
            return None

    def _load_vendors(self):
        try:
            self._sink.logger.info("Fetching Vendors via REST SuiteQL...")
            vendors = self._sink.get_reference_vendors_rest()
            if vendors:
                return vendors
        except Exception as e:
            self._sink._check_exception(e, "Vendors")

        try:
            self._sink.logger.info("Fetching Vendors via SOAP")
            return self._sink.ns_client.entities["Vendors"].get_all(
                [
                    "companyName",
                    "firstName",
                    "lastName",
                    "altName",
                    "isInactive",
                    "externalId",
                    "name",
                    "subsidiary",
                ],
                page_size=100,
            )
        except Exception as e:
            self._sink._check_exception(e, "Vendors")
            return None

    def _load_subsidiaries(self):
        try:
            return self._sink.ns_client.entities["Subsidiaries"].get_all(
                ["name"], page_size=100
            )
        except Exception as e:
            self._sink._check_exception(e, "Subsidiaries")
            return None
