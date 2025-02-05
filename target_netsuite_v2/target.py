import json
import os

from datetime import datetime
from netsuitesdk.internal.exceptions import NetSuiteRequestError
from pathlib import PurePath
from pendulum import parse
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue
from target_netsuite_v2.netsuite import NetSuite
from target_netsuite_v2.sink.vendor_sink import VendorSink
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

    SINK_TYPES = [VendorSink]

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)
        self.ns_client = self.get_ns_client()
        self.reference_data = self.get_reference_data()

    def get_ns_client(self):
        ns_account = self.config.get("ns_account")
        ns_consumer_key = self.config.get("ns_consumer_key")
        ns_consumer_secret = self.config.get("ns_consumer_secret")
        ns_token_key = self.config.get("ns_token_key")
        ns_token_secret = self.config.get("ns_token_secret")
        is_sandbox = self.config.get("is_sandbox")

        self.logger.info(f"Starting netsuite connection")
        ns = NetSuite(
            ns_account=ns_account,
            ns_consumer_key=ns_consumer_key,
            ns_consumer_secret=ns_consumer_secret,
            ns_token_key=ns_token_key,
            ns_token_secret=ns_token_secret,
            is_sandbox=is_sandbox,
        )

        ns.connect_tba(caching=False)
        self.logger.info(f"Successfully created netsuite connection..")
        return ns.ns_client

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
        # reference_data["Vendors"] = self.ns_client.entities["Vendors"].get_all(["entityId", "companyName"])
        reference_data["Subsidiaries"] = self.ns_client.entities["Subsidiaries"].get_all(["name"])
        # reference_data["Classifications"] = self.ns_client.entities["Classifications"].get_all(["name"])
        # reference_data["Items"] = self.ns_client.entities["Items"].get_all(["itemId"])
        reference_data["Currencies"] = self.ns_client.entities["Currencies"].get_all()
        # reference_data["Departments"] = self.ns_client.entities["Departments"].get_all(["name"])
        # reference_data["Customer"] = self.ns_client.entities["Customer"].get_all(["name", "companyName", "entityId"])
        try:
            reference_data["Locations"] = self.ns_client.entities["Locations"].get_all(["name"])
        except NetSuiteRequestError as e:
            message = e.message.replace("error", "failure").replace("Error", "")
            self.logger.warning(f"It was not possible to retrieve Locations data: {message}")
        reference_data["Accounts"] = self.ns_client.entities["Accounts"](self.ns_client.ns_client).get_all(["acctName", "acctNumber", "subsidiaryList"])

        if self.config.get("snapshot_hours"):
            reference_data["write_date"] = datetime.utcnow().isoformat()
            os.makedirs("snapshots", exist_ok=True)
            with open('snapshots/reference_data.json', 'w') as outfile:
                json.dump(reference_data, outfile)

        return reference_data


if __name__ == "__main__":
    TargetNetsuiteV2.cli()