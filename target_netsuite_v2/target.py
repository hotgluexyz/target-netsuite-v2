import json
import os

from datetime import datetime
from pathlib import PurePath
from pendulum import parse
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue
from target_netsuite_v2.sink.vendor_sink import VendorSink
from target_netsuite_v2.sink.account_sink import AccountSink
from target_netsuite_v2.sink.customer_sink import CustomerSink
from target_netsuite_v2.suite_talk_client import SuiteTalkRestClient
from typing import List, Optional, Union

class TargetNetsuiteV2(TargetHotglue):
    """netsuite-v2 target class."""

    name = "target-netsuite-v2"
    config_jsonschema = th.PropertiesList(
        th.Property("ns_consumer_key", th.StringType),
        th.Property("ns_consumer_secret", th.StringType),
        th.Property("ns_token_key", th.StringType),
        th.Property("ns_token_secret", th.StringType),
        th.Property("ns_account", th.StringType)
    ).to_dict()

    SINK_TYPES = [VendorSink, AccountSink, CustomerSink]

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)
        self.suite_talk_client = self.get_ns_client()
        self.reference_data = self.get_reference_data()

    def get_ns_client(self):
        netsuite_config = {
            "ns_consumer_key": self.config["ns_consumer_key"],
            "ns_consumer_secret": self.config["ns_consumer_secret"],
            "ns_token_key": self.config["ns_token_key"],
            "ns_token_secret": self.config["ns_token_secret"],
            "ns_account": self.config["ns_account"]
        }
        return SuiteTalkRestClient(netsuite_config)

    def get_reference_data(self):
        if self.config.get("snapshot_hours"):
            try:
                with open(f'{self.config.get("snapshot_dir", "snapshots")}/reference_data.json') as json_file:
                    reference_data = json.load(json_file)
                    if reference_data.get("write_date"):
                        last_run = parse(reference_data["write_date"])
                        last_run = last_run.replace(tzinfo=None)
                        if (datetime.utcnow()-last_run).total_hours()<int(self.config.get("snapshot_hours")):
                            return reference_data
            except:
                self.logger.info(f"Snapshot not found or not readable.")

        self.logger.info(f"Reading data from API...")
        reference_data = {}

        _, _, vendors = self.suite_talk_client.get_reference_data("vendor")
        reference_data["Vendors"] = vendors

        _, _, subsidiaries = self.suite_talk_client.get_reference_data("subsidiary")
        reference_data["Subsidiaries"] = subsidiaries

        _, _, classifications = self.suite_talk_client.get_reference_data("classification")
        reference_data["Classifications"] = classifications

        _, _, currencies = self.suite_talk_client.get_reference_data("currency")
        reference_data["Currencies"] = currencies

        _, _, departments = self.suite_talk_client.get_reference_data("department")
        reference_data["Departments"] = departments

        _, _, locations = self.suite_talk_client.get_reference_data("location")
        reference_data["Locations"] = locations

        _, _, accounts = self.suite_talk_client.get_reference_data("account")
        reference_data["Accounts"] = accounts

        # Batch specific reference data is not currently being written to the snapshot since it is not fetched here
        # But is instead lazily fetched per batch
        if self.config.get("snapshot_hours"):
            reference_data["write_date"] = datetime.utcnow().isoformat()
            os.makedirs("snapshots", exist_ok=True)
            with open('snapshots/reference_data.json', 'w') as outfile:
                json.dump(reference_data, outfile)

        return reference_data


if __name__ == "__main__":
    TargetNetsuiteV2.cli()