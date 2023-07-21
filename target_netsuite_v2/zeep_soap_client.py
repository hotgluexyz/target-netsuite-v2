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