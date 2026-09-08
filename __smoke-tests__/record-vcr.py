import json
import os

import vcr

from hotglue_smoke_test.vcr.json_body_comparator import JsonBodyComparator
from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetNetsuiteV2TestRunner(VCRTargetTestRunner):
    def module(self) -> str:
        return "target_netsuite_v2"

    def launch(self):
        from target_netsuite_v2.target import TargetNetsuiteV2

        TargetNetsuiteV2.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        test_config = {}
        if os.path.exists(self.test_config_path):
            with open(self.test_config_path) as config_file:
                test_config = json.load(config_file)

        def body_with_ignore(r1, r2):
            if r1.path != "/services/rest/query/v1/suiteql" and r2.path != "/services/rest/query/v1/suiteql":
                JsonBodyComparator(test_config).compare(r1, r2)

        my_vcr = vcr.VCR()
        my_vcr.register_matcher("body_with_ignore", body_with_ignore)

        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=["authorization"],
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body_with_ignore"],
        )


if __name__ == "__main__":
    TargetNetsuiteV2TestRunner.main()
