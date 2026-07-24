"""netsuite-v2 target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_netsuite_v2.sinks import (
    netsuiteV2Sink,
)


class TargetNetsuiteV2(TargetHotglue):
    """Sample target for netsuite-v2."""

    name = "target-netsuite-v2"
    config_jsonschema = th.PropertiesList(
        th.Property("ns_consumer_key", th.StringType),
        th.Property("ns_consumer_secret", th.StringType),
        th.Property("ns_token_key", th.StringType),
        th.Property("ns_token_secret", th.StringType),
        th.Property("ns_account", th.StringType)
    ).to_dict()

    SINK_TYPES = [netsuiteV2Sink]
    
    def get_sink_class(self, stream_name: str):
        return netsuiteV2Sink


if __name__ == "__main__":
    TargetNetsuiteV2.cli()