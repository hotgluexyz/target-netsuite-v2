import abc
import json
import hashlib

from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBaseSink
from target_hotglue.common import HGJSONEncoder
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
        self.suite_talk_client = self._target.suite_talk_client

    def record_exists(self, record: dict, context: dict) -> bool:
        return bool(record.get("internalId"))

    def build_record_hash(self, record: dict):
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def get_existing_state(self, hash: str):
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        if existing_state:
            self.latest_state["summary"][self.name]["existing"] += 1

        return existing_state

class NetSuiteBatchSink(NetSuiteBaseSink, BatchSink):
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        The `context["records"]` list will contain all records from the given batch
        context.

        Args:
            context: Stream partition or context dictionary.
        """
        if not self.latest_state:
            self.init_state()

        batch_records = context["records"]

        reference_data = self.get_batch_reference_data(context)

        for record in batch_records:
            self.process_batch_record(record, reference_data)

    def get_batch_reference_data(self, context: dict) -> dict:
        """Get the reference data for a batch

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A dict containing batch specific reference data.
        """
        return self._target.reference_data

    def process_batch_record(self, record: dict, reference_data: dict):
        """Process a record in the batch

        Preprocess the record to map it to the desired payload.
        Capture state updates, and upsert the record to the target

        Args:
            record: Individual raw record in the stream.
            reference_data: A dictionary containing all reference_data necessary for a batch.
        """
        preprocessed = self.preprocess_batch_record(record, reference_data)
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

    @abc.abstractmethod
    def preprocess_batch_record(self, record: dict) -> dict:
        """Preprocess a batch with the given batch context.

        This method must be overridden.
        Map the raw record to a dictionary for use in the API payload.

        Args:
            record: Individual raw record in the stream.
        """
        pass

    def upsert_record(self, record: dict, context: dict):
        state = {}

        if self.record_exists(record, context):
            id, success, error_message = self.suite_talk_client.update_record(self.record_type, record['internalId'], record)
        else:
            id, success, error_message = self.suite_talk_client.create_record(self.record_type, record)

        if error_message:
            state["error"] = error_message

        return id, success, state

    def get_primary_records_for_batch(self, context) -> dict:
        """Get the reference records for the sinks record type for a given batch"""
        raw_records = context["records"]

        ids = []
        external_ids = []

        for record in raw_records:
            if record.get("id"):
                ids.append(record["id"])

            if record.get("externalId"):
                external_ids.append(record["externalId"])

        _, _, items = self.suite_talk_client.get_reference_data(
            self.record_type,
            record_ids=ids,
            external_ids=external_ids
        )

        return { self.name: items }
