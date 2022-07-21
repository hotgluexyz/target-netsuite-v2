"""netsuite-v2 target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_netsuite_v2.sinks import (
    netsuiteV2Sink,
)


class TargetNetsuiteV2(Target):
    """Sample target for netsuite-v2."""

    name = "target-netsuite-v2"
    config_jsonschema = th.PropertiesList(
        th.Property("ns_consumer_key", th.StringType),
        th.Property("ns_consumer_secret", th.StringType),
        th.Property("ns_token_key", th.StringType),
        th.Property("ns_token_secret", th.StringType),
        th.Property("ns_account", th.StringType)
    ).to_dict()
    default_sink_class = netsuiteV2Sink


if __name__ == "__main__":
    TargetNetsuiteV2.cli()