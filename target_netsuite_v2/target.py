from target_hotglue.target import TargetHotglue
from singer_sdk import typing as th
from typing import List, Optional, Union
from pathlib import PurePath

from target_netsuite_v2.sinks import (
    netsuiteV2Sink,
)


class TargetNetsuiteV2(TargetHotglue):
    """netsuite-v2 target class."""
    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

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