"""Colocated smoke-test runner for target-netsuite-v2 (hotglue-smoke-test)."""

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetNetsuiteV2TestRunner(VCRTargetTestRunner):
    # Integration / lookup field names (not display names or dimension labels).
    PRESERVE_KEYS = {
        "name",
        "accountNumber",
        "billExternalId",
        "billNumber",
        "customerNumber",
        "entityId",
        "entityid",
        "externalId",
        "invoiceNumber",
        "itemId",
        "itemNumber",
        "journalEntryNumber",
        "number",
        "paymentNumber",
        "purchaseOrderNumber",
        "taxCode",
        "vendorNumber",
        "locationName",
        "itemName",
        "externalid",
       "tranId",
       "tranid",
       "itemid",
       "memo",
       "subsidiaryid",
       "status",
       "type",
       "category",
       "variantName",
       "vendorName",
       "companyName",
       "salesRepName",
       "departmentName",
       "subsidiaryName",
       "employeeName",
       "city",
       "state",
      # net suite error details 
       "detail",
       "o:errorCode",
       "title"
    }

    PRESERVE_VALUES = {
        "TEST-EXTERNAL-ID",
        "NEW-PO-EXTERNAL-ID",
        "NEW-PO-NUMBER",
        "PO-N-Test-3102",
        "TEST-PO-N-3301",
        "TEST-EXT-PO-3301",
        "TEST-BILL-N-10001",
        "TEST-EXT-BILL-10001",
        "TEST Cust Name 1206",
        "TEST-CUST-N-1206",
        "TF3 Company Name",
        "Cash",
        "Test500",
        "Item_702",
        "TestVendor",
        "German cars",
        "asset",
        "invoice",
        "billing",
        "cogs",
        "home",
        "mobile",
        "shipping",
        "unknown",
        "Open",
        "true",
        "test",
        "line1",
        "Credit",
        "Debit",
        "Invalid",
        "test@example.com",
    }

    def module(self) -> str:
        return "target_netsuite_v2"

    def launch(self):
        from target_netsuite_v2.target import TargetNetsuiteV2

        TargetNetsuiteV2.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetNetsuiteV2TestRunner.main()
