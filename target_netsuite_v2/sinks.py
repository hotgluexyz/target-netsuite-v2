import json
import requests
import hashlib

from singer_sdk.exceptions import FatalAPIError
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBaseSink, HotglueSink
from target_hotglue.common import HGJSONEncoder
from target_netsuite_v2.suite_talk_client import SuiteTalkRestClient
from typing import Dict, List, Optional

class NetSuiteBaseSink(HotglueBaseSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        netsuite_config = {
            "ns_consumer_key": self.config["ns_consumer_key"],
            "ns_consumer_secret": self.config["ns_consumer_secret"],
            "ns_token_key": self.config["ns_token_key"],
            "ns_token_secret": self.config["ns_token_secret"],
            "ns_account": self.config["ns_account"]
        }
        self.suite_talk_client = SuiteTalkRestClient(netsuite_config)

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("internalId"))

    def response_error_message(self, response: requests.Response) -> str:
        return json.dumps(response.json().get("o:errorDetails"))

    def build_record_hash(self, record: dict):
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def get_existing_state(self, hash: str):
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        if existing_state:
            self.latest_state["summary"][self.name]["existing"] += 1

        return existing_state

class NetSuiteSink(NetSuiteBaseSink, HotglueSink):
    def upsert_record(self, record: dict, context: dict):
        if self.record_exists(record, context):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], record)
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

        if not success:
            raise FatalAPIError(error_message)

        return id, success, dict()

class NetSuiteBatchSink(NetSuiteBaseSink, BatchSink):
    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        for record in raw_records:
            self.process_batch_record(record)

    def process_batch_record(self, record):
        preprocessed = self.preprocess_batch_record(record)
        hash = self.build_record_hash(preprocessed)
        existing_state = self.get_existing_state(hash)
        external_id = preprocessed.get("externalId")

        if existing_state:
            self.update_state(existing_state, is_duplicate=True)
            return

        id, success, state = self.upsert_record(preprocessed, {})

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if external_id:
            state["externalId"] = external_id

        self.update_state(state)

    def upsert_record(self, record: dict, context: dict):
        state = {}

        if self.record_exists(record, context):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, id, record)
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

        if error_message:
            state["error"] = error_message

        return id, success, state
