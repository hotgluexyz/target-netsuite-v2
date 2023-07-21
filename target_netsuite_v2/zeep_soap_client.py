"""Custom client handling, including NetsuiteStream base class."""

import base64
import hashlib
import hmac
import random
from datetime import datetime
from decimal import Decimal
from time import time
from typing import Iterable, Optional

import backoff
from backports.cached_property import cached_property

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from zeep import Client
from zeep.cache import SqliteCache
from zeep.helpers import serialize_object
from zeep.transports import Transport
import json


class NetsuiteSoapClient:
    """Stream class for Netsuite streams."""
    def __init__(self,config):
        self.config = config

    @cached_property
    def account(self):
        return self.config["ns_account"].replace("_", "-")

    @cached_property
    def wsdl_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "wsdl/v2022_2_0/netsuite.wsdl"
        )

    @cached_property
    def datacenter_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "services/NetSuitePort_2022_2"
        )

    @property
    def client(self):
        return Client(self.wsdl_url)

    @cached_property
    def service_proxy(self):
        proxy_url = "{urn:platform_2022_2.webservices.netsuite.com}NetSuiteBinding"
        return self.client.create_service(proxy_url, self.datacenter_url)

    def search_client(self, type_name):
        for ns_type in self.client.wsdl.types.types:
            if ns_type.name and ns_type.name == type_name:
                return ns_type

    @cached_property
    def ns_type(self):
        return self.search_client(self.name)

    @cached_property
    def search_type(self):
        search_type_name = self.search_type_name or self.name + "SearchBasic"
        return self.search_client(search_type_name)

    def generate_token_passport(self):
        consumer_key = self.config["ns_consumer_key"]
        consumer_secret = self.config["ns_consumer_secret"]
        token_key = self.config["ns_token_key"]
        token_secret = self.config["ns_token_secret"]
        account = self.config["ns_account"]

        nonce = "".join([str(random.randint(0, 9)) for _ in range(20)])
        timestamp = str(int(datetime.now().timestamp()))
        key = f"{consumer_secret}&{token_secret}".encode(encoding="ascii")
        msg = "&".join([account, consumer_key, token_key, nonce, timestamp])
        msg = msg.encode(encoding="ascii")

        # compute the signature
        hashed_value = hmac.new(key, msg=msg, digestmod=hashlib.sha256)
        dig = hashed_value.digest()
        signature_value = base64.b64encode(dig).decode()

        passport_signature = self.search_client("TokenPassportSignature")
        signature = passport_signature(signature_value, algorithm="HMAC-SHA256")

        passport = self.search_client("TokenPassport")
        return passport(
            account=account,
            consumerKey=consumer_key,
            token=token_key,
            nonce=nonce,
            timestamp=timestamp,
            signature=signature,
        )

    def build_headers(self, include_search_preferences: bool = False):
        soapheaders = {}
        soapheaders["tokenPassport"] = self.generate_token_passport()
        if include_search_preferences:
            search_preferences = self.search_client("SearchPreferences")
            preferences = {
                "bodyFieldsOnly": False,
                "pageSize": self.page_size,
                "returnSearchColumns": True,
            }
            soapheaders["searchPreferences"] = search_preferences(**preferences)
        return soapheaders

    @backoff.on_exception(backoff.expo, RetriableAPIError, max_tries=5, factor=2)
    def request(self, name, *args, **kwargs):
        method = getattr(self.service_proxy, name)
        # call the service:
        is_search = name == "search"
        headers = self.build_headers(include_search_preferences=is_search)

        request_start_time = time()
        response = method(*args, _soapheaders=headers, **kwargs)
        request_duration = time() - request_start_time

        response_body_attrs = list(vars(response.body)["__values__"].keys())
        request_type = next(k for k in response_body_attrs if k in self.valid_requests)

        result = getattr(response.body, request_type)

        if hasattr(result, "totalRecords"):
            page_size = result.totalRecords
        elif result.totalPages == result.pageIndex:
            page_size = result.totalRecords - (result.pageIndex - 1) * result.pageSize
        else:
            page_size = result.pageSize

        request_status = "SUCCESS" if result.status.isSuccess else "ERROR"
        extra_tags = dict(page_size=page_size)
        metric = {
            "type": "timer",
            "metric": "request_duration",
            "value": round(request_duration, 4),
            "tags": {
                "object": self.name,
                "status": request_status,
            },
        }
        self._write_metric_log(metric=metric, extra_tags=extra_tags)

        self.validate_response(result)
        return result

    
    def validate_response(self, result) -> None:
        """Validate zeep response."""
        if not result.status.isSuccess:
            status = result.status.statusDetail[0]
            if status.code in RETRYABLE_ERRORS:
                msg = self.response_error_message(status)
                raise RetriableAPIError(msg, status)
            else:
                msg = self.response_error_message(status)
                raise FatalAPIError(msg)

    def response_error_message(self, status) -> str:
        """Build error message for invalid http statuses."""
        return f'{status.code} error for {self.name}: "{status.message}"'



def main():
    ns = NetsuiteStream()
    soap_client = ns.client
        
    passport = ns.generate_token_passport()        
    app_info = AppInfo(applicationId=ns.config.get('account'))             
    login = soap_client.service.login(passport=passport, _soapheaders={'applicationInfo': app_info}) 

if __name__ == '__main__':
    main()