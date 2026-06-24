"""netsuite-v2 target sink class, which handles writing streams."""

from target_hotglue.sinks import HotglueSink
from target_netsuite_v2.netsuite import NetSuite
from target_netsuite_v2.zeep_soap_client import NetsuiteSoapClient

from netsuitesdk.internal.exceptions import NetSuiteRequestError
import json
import os
import requests
import base64

from difflib import SequenceMatcher
from heapq import nlargest as _nlargest
from pendulum import parse
from datetime import datetime

class netsuiteSoapV2Sink(HotglueSink):
    """netsuite-v2 target sink class."""

    def get_close_matches(self, word, possibilities, n=20, cutoff=0.7):
        if not n >  0:
            raise ValueError("n must be > 0: %r" % (n,))
        if not 0.0 <= cutoff <= 1.0:
            raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
        result = []
        s = SequenceMatcher()
        s.set_seq2(word)
        for x in possibilities:
            s.set_seq1(x)
            if s.real_quick_ratio() >= cutoff and \
            s.quick_ratio() >= cutoff and \
            s.ratio() >= cutoff:
                result.append((s.ratio(), x))
        result = _nlargest(n, result)

        return {v: k for (k, v) in result}

    def get_by_fully_qualified_name(self, name, data):
        if not name:
            return None

        parts = [part.strip() for part in name.split(":") if part.strip()]
        if not parts:
            return None

        if len(parts) == 1:
            for record in data:
                if record.get("name") == parts[0]:
                    return [record]
            return None

        by_internal_id = {
            record["internalId"]: record
            for record in data
            if record.get("internalId")
        }

        def path_from_root(record):
            path = []
            current = record
            visited = set()
            while current:
                internal_id = current.get("internalId")
                if internal_id:
                    if internal_id in visited:
                        break
                    visited.add(internal_id)
                path.insert(0, current.get("name"))
                parent = current.get("parent") or {}
                if not isinstance(parent, dict):
                    break
                parent_id = parent.get("internalId")
                if parent_id and parent_id in by_internal_id:
                    current = by_internal_id[parent_id]
                elif parent.get("name"):
                    path.insert(0, parent.get("name"))
                    break
                else:
                    break
            return path

        leaf_name = parts[-1]
        for record in data:
            if record.get("name") != leaf_name:
                continue
            if path_from_root(record) == parts:
                return [record]
        return None

    def _lookup_subsidiary(self, subsidiary_name, context):
        subsidiaries_ref = context["reference_data"].get("Subsidiaries") or []
        if not subsidiaries_ref:
            return None
        match = next(
            (
                subsidiary
                for subsidiary in subsidiaries_ref
                if subsidiary.get("name") == subsidiary_name
            ),
            None,
        )
        if not match:
            return None
        return {
            "name": match.get("name"),
            "externalId": match.get("externalId"),
            "internalId": match.get("internalId"),
        }

    def _get_custom_field_type_and_value(self, script_id, value, context, rest_post_method):
        field_type_map = {
            "Check Box": "Boolean",
            "Date": "Date",
            "Date/Time": "DateTime",
            "Time Of Day": "Time",
            "List/Record": "Select",
        }
        custom_fields = context.get("reference_data", {}).get("CustomFields", {})
        custom_field = custom_fields.get(script_id.upper())

        if not custom_field:
            raise Exception(f"Error parsing custom field, scriptid '{script_id}' is not valid.")

        return field_type_map.get(custom_field.get("fieldValueType", ""), "String"), value

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
        self.ns_client = ns.ns_client
        self.logger.info(f"Successfully created netsuite connection..")
    
    def _check_exception(self, exception, stream_name):
        exception_string = exception.__str__()
                
        if "INSUFFICIENT_PERMISSION" in exception_string or "Your current role does not have permission to perform this action" in exception_string:
            self.logger.warning(f"Insufficient permissions to access content for {stream_name}. Skipping...")
            return

        raise exception

    def process_file(self, attachments, record):
        if not attachments:
            return []

        attachments_folder_name = record.get("attachments_folder")
        if not attachments_folder_name:
            raise Exception(f"Failed to send attachments for record {record}, unable to find a field that can be used as an external ID.")
        
        created_folder = self.ns_client.entities["Folders"].post(
            {
                "externalId": attachments_folder_name,
                "name": attachments_folder_name
            }
        )
        created_folder = dict(created_folder)
        attachments_ids = []

        for attachment in attachments:
            url = attachment.get("url")
            name = attachment.get("name")
            if url and name:
                self.logger.info(f"Downloading file {url}")
                response = requests.get(url)
                content = base64.b64encode(response.content)
                content = content.decode()
                self.logger.info(f"Uploading file {name}")
                uploaded_file = self.ns_client.entities["Files"].post({
                    "externalId": name,
                    "name": name,
                    "content": content,
                    "folder": {
                            "name": attachments_folder_name,
                            "internalId": created_folder.get("internalId"),
                            "externalId": attachments_folder_name,
                            "type": "folder"
                        }
                    }
                )

                uploaded_file = dict(uploaded_file)
                attachments_ids.append(uploaded_file.get("internalId"))
        
        return attachments_ids

    def get_reference_data(self):
        if self._target.reference_data:
            return self._target.reference_data
        
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

        try:
            reference_data["Classifications"] = self.ns_client.entities["Classifications"].get_all(["name"])
        except Exception as e:
            self._check_exception(e, "Classifications")

        try:
            reference_data["Currencies"] = self.ns_client.entities["Currencies"].get_all()
        except Exception as e:
            self._check_exception(e, "Currencies")
        
        try:
            reference_data["Departments"] = self.ns_client.entities["Departments"].get_all(["name"])
        except Exception as e:
            self._check_exception(e, "Departments")

        try:
            reference_data["Accounts"] = self.ns_client.entities["Accounts"](self.ns_client.ns_client).get_all(["acctName", "acctNumber", "subsidiaryList", "acctType", "class", "department", "isInactive", "location"], page_size=100)
        except Exception as e:
            if "You need  the 'Lists -> Documents and Files' permission" in str(e):
                self.logger.info(f"Permissions for Documents and Files missing. Attempting to get Accounts with body_fields_only=True")
                reference_data["Accounts"] = self.ns_client.entities["Accounts"](self.ns_client.ns_client, body_fields_only=True).get_all(["acctName", "acctNumber", "subsidiaryList", "acctType", "class", "department", "isInactive", "location"], page_size=100)
                self.ns_client.ns_client._search_preferences.bodyFieldsOnly = False
            else:
                self._check_exception(e, "Accounts")

        try:
            reference_data["Locations"] = self.ns_client.entities["Locations"].get_all(["name", "subsidiaryList", "isInactive"], page_size=100)
            self.logger.info(f"Locations: {reference_data['Locations']}")
        except NetSuiteRequestError as e:
            message = e.message.replace("error", "failure").replace("Error", "")
            self.logger.warning(f"It was not possible to retrieve Locations data: {message}")
        except Exception as e:
            self._check_exception(e, "Locations")

        try:
            reference_data["Customers"] = self.ns_client.entities["Customer"](self.ns_client.ns_client).get_all(
                ["companyName", "entityId", "externalId", "isInactive", "name", "subsidiary"],
                page_size=100,
            )
            reference_data["Customer"] = reference_data["Customers"]
        except Exception as e:
            self._check_exception(e, "Customers")

        try:
            self.logger.info("Fetching Jobs via REST SuiteQL...")
            reference_data["Jobs"] = self.get_reference_jobs_rest()
        except Exception as e:
            self._check_exception(e, "Jobs")

        try:
            self.logger.info("Fetching Vendors via REST SuiteQL...")
            reference_data["Vendors"] = self.get_reference_vendors_rest()
        except Exception as e:
            self._check_exception(e, "Vendors")
        
        if "Vendors" not in reference_data:
            try:
                self.logger.info("Fetching Vendors via SOAP")
                reference_data["Vendors"] = self.ns_client.entities["Vendors"].get_all(["companyName", "firstName", "lastName", "altName", "isInactive", "externalId","name", "subsidiary"], page_size=100)
            except Exception as e:
                self._check_exception(e, "Vendors")

        try:
            reference_data["Subsidiaries"] = self.ns_client.entities["Subsidiaries"].get_all(["name"], page_size=100)
        except Exception as e:
            self._check_exception(e, "Subsidiaries")

        reference_data["CustomFields"] = self._fetch_all_custom_fields()

        if self.config.get("snapshot_hours"):
            reference_data["write_date"] = datetime.utcnow().isoformat()
            os.makedirs("snapshots", exist_ok=True)
            with open('snapshots/reference_data.json', 'w') as outfile:
                json.dump(reference_data, outfile)


        # Cache reference data in target
        self._target.reference_data = reference_data
        return reference_data

    def process_journal_entry(self, context, record):
        context["reference_data"] = self.reference_data
        subsidiaries = {}
        line_items = []
        for line in record.get("journalLines", record.get("lines", [])):
            journal_entry_line = {}

            if self.reference_data.get("Accounts"):
                acct_data = None
                if line.get("accountId"):
                    acct_data = [
                        a
                        for a in self.reference_data["Accounts"]
                        if a["internalId"] == line["accountId"]
                    ]
                elif line.get("accountNumber") and not line.get("accountId"):
                    acct_num = str(line["accountNumber"])
                    acct_data = [
                        a
                        for a in self.reference_data["Accounts"]
                        if a["acctNumber"] == acct_num
                    ]

                if not acct_data:
                    raise Exception(
                        f"AccountId '{line.get('accountId')}' and/or accountNumber {line.get('accountNumber')} were not provided or not valid."
                    )

                acct_data = acct_data[0]
                journal_entry_line = {
                    "account": {
                        "name": acct_data.get("acctName"),
                        "externalId": acct_data.get("externalId"),
                        "internalId": acct_data.get("internalId"),
                    }
                }

                subsidiary_internal_id = line.get("subsidiary") or line.get("subsidiaryId")
                if subsidiary_internal_id:
                    subsidiary = {
                        "name": None,
                        "internalId": subsidiary_internal_id,
                        "externalId": None,
                        "type": None,
                    }
                elif line.get("subsidiaryName"):
                    subsidiary = self._lookup_subsidiary(line["subsidiaryName"], context)
                    if not subsidiary:
                        raise Exception(f"Subsidiary with name '{line['subsidiaryName']}' was not found.")
                else:
                    subsidiary = acct_data["subsidiaryList"]
                    if subsidiary:
                        subsidiary = subsidiary[0]
                    else:
                        raise Exception(
                            f"No subsidiary was provided for line {line} and account subsidiaries couldn't be fetched because of missing permission."
                        )
                if subsidiary:
                    if line["postingType"].lower() == "credit":
                        subsidiaries["toSubsidiary"] = subsidiary
                    elif line["postingType"].lower() == "debit":
                        subsidiaries["subsidiary"] = subsidiary
                    else:
                        raise Exception('Posting Type must be "credit" or "debit"')
            else:
                raise Exception("We failed to fetch Accounts from NetSuite. Please validate permissions.")

            if self.reference_data.get("Classifications") and line.get("className"):
                class_names = [c["name"] for c in self.reference_data["Classifications"]]
                class_name = self.get_close_matches(line["className"], class_names)
                if class_name:
                    class_name = max(class_name, key=class_name.get)
                    class_data = [c for c in self.reference_data["Classifications"] if c["name"] == class_name]
                    if class_data:
                        class_data = class_data[0]
                        journal_entry_line["class"] = {
                            "name": class_data.get("name"),
                            "externalId": class_data.get("externalId"),
                            "internalId": class_data.get("internalId"),
                        }

            department_name = line.get("departmentName") or line.get("department")
            if self.reference_data.get("Departments") and department_name:
                dept_data = self.get_by_fully_qualified_name(department_name, self.reference_data["Departments"])
                if not dept_data:
                    dept_names = [d["name"] for d in self.reference_data["Departments"]]
                    dept_name = self.get_close_matches(department_name, dept_names)
                    if dept_name:
                        dept_name = max(dept_name, key=dept_name.get)
                        dept_data = [d for d in self.reference_data["Departments"] if d["name"] == dept_name]

                if dept_data:
                    dept_data = dept_data[0]
                    journal_entry_line["department"] = {
                        "name": dept_data.get("name"),
                        "externalId": dept_data.get("externalId"),
                        "internalId": dept_data.get("internalId"),
                    }

            location_name = line.get("locationName") or line.get("location")
            if line.get("locationId"):
                journal_entry_line["location"] = {"internalId": line.get("locationId")}
            elif self.reference_data.get("Locations") and location_name:
                loc_data = [l for l in self.reference_data["Locations"] if l["name"] == location_name]
                if loc_data:
                    loc_data = loc_data[0]
                    journal_entry_line["location"] = {
                        "name": loc_data.get("name"),
                        "externalId": loc_data.get("externalId"),
                        "internalId": loc_data.get("internalId"),
                    }

            customers = self.reference_data.get("Customer") or self.reference_data.get("Customers") or []
            if customers:
                customer_data = []
                if line.get("customerId"):
                    customer_data = [c for c in customers if c["internalId"] == line["customerId"]]
                if line.get("customerName") and not customer_data:
                    customer_data = [c for c in customers if c.get("entityId") == line["customerName"]]
                    if not customer_data:
                        customer_names = []
                        for c in customers:
                            if "name" in c and c["name"]:
                                customer_names.append(c["name"])
                            elif c.get("companyName"):
                                customer_names.append(c["companyName"])
                        customer_name = self.get_close_matches(line["customerName"], customer_names, n=2, cutoff=0.95)
                        if customer_name:
                            customer_name = max(customer_name, key=customer_name.get)
                            customer_data = [
                                c
                                for c in customers
                                if c.get("name") == customer_name or c.get("companyName") == customer_name
                            ]

                if customer_data:
                    customer_data = customer_data[0]
                    journal_entry_line["entity"] = {
                        "externalId": customer_data.get("externalId"),
                        "internalId": customer_data.get("internalId"),
                    }

            amount = 0 if not line["amount"] else abs(round(line["amount"], 2))
            if line["postingType"].lower() == "credit":
                journal_entry_line["credit"] = amount
            elif line["postingType"].lower() == "debit":
                journal_entry_line["debit"] = amount

            if "description" in line:
                journal_entry_line["memo"] = line["description"]

            custom_field_values = []
            if line.get("asset"):
                custom_field_values.append({"type": "Select", "scriptId": "custcol_far_trn_relatedasset", "value": line["asset"]})

            custom_fields = line.get("customFields") or []
            if isinstance(custom_fields, str):
                custom_fields = json.loads(custom_fields)
            if not isinstance(custom_fields, list):
                raise Exception(f"Invalid customFields. Expecting a list of name/value pairs. Received: {custom_fields}")

            for entry in custom_fields:
                value = entry.get("value")
                ns_id = entry.get("name")
                if value is not None:
                    field_type = entry.get("type")
                    if not field_type:
                        field_type, value = self._get_custom_field_type_and_value(ns_id, value, context, self.rest_post)
                    custom_field_values.append({"type": field_type, "scriptId": ns_id, "value": value})

            if custom_field_values:
                journal_entry_line["customFieldList"] = custom_field_values

            line_items.append(journal_entry_line)

        if record.get("currency") and not self.reference_data.get("Currencies"):
            raise Exception("A currency was provided in the payload, but we failed to fetch Currencies from NetSuite. Please validate permissions.")

        if self.reference_data.get("Currencies") and record.get("currency"):
            currency_data = [
                c for c in self.reference_data["Currencies"] if c["symbol"] == record["currency"]
            ]
            if currency_data:
                currency_data = currency_data[0]
                currency_ref = {
                    "name": currency_data.get("symbol"),
                    "externalId": currency_data.get("externalId"),
                    "internalId": currency_data.get("internalId"),
                }
            else:
                currency_ref = None
        else:
            currency_ref = None

        subsidiary = None
        record_subsidiary_internal_id = record.get("subsidiary") or record.get("subsidiaryId")
        if record_subsidiary_internal_id:
            subsidiary = {"internalId": record_subsidiary_internal_id}
        elif record.get("subsidiaryName"):
            subsidiary = self._lookup_subsidiary(record["subsidiaryName"], context)
            if not subsidiary:
                raise Exception(f"Subsidiary with name '{record['subsidiaryName']}' was not found.")
        elif len(subsidiaries) > 1 and subsidiaries["subsidiary"] == subsidiaries["toSubsidiary"]:
            subsidiary = subsidiaries["subsidiary"]

        created_date = parse(record["transactionDate"]) if "transactionDate" in record else None
        journal_entry = {
            "createdDate": created_date,
            "tranDate": created_date,
            "lineList": line_items,
            "currency": currency_ref,
            "subsidiary": subsidiary,
        }

        if record.get("id"):
            journal_entry["externalId"] = record["id"]
        else:
            raise Exception(f"Invalid Journal Entry: id is a required field. {record}")

        if "journalDesc" in record:
            journal_entry["memo"] = "" if not record["journalDesc"] else record["journalDesc"]

        record_custom_fields = []
        custom_fields = record.get("customFields") or []
        if isinstance(custom_fields, str):
            custom_fields = json.loads(custom_fields)
        if not isinstance(custom_fields, list):
            raise Exception(f"Invalid customFields. Expecting a list of name/value pairs. Received: {custom_fields}")

        for entry in custom_fields:
            value = entry.get("value")
            ns_id = entry.get("name")
            if value is not None:
                field_type = entry.get("type")
                if not field_type:
                    field_type, value = self._get_custom_field_type_and_value(ns_id, value, context, self.rest_post)
                record_custom_fields.append({"type": field_type, "scriptId": ns_id, "value": value})
        if record_custom_fields:
            journal_entry["customFieldList"] = record_custom_fields

        return journal_entry

    def process_customer_payment(self, context, record):
        # Get the currency ID
        if self.reference_data.get("Currencies") and record.get("currency"):
            currency_data = [
                c for c in self.reference_data["Currencies"] if c["symbol"] == record["currency"]
                ]
            if currency_data:
                currency_data = currency_data[0]
                currency_ref = {
                    "name": currency_data.get("symbol"),
                    "externalId": currency_data.get("externalId"),
                    "internalId": currency_data.get("internalId"),
                }
        else:
            currency_ref = None

        if "transactionDate" in record.keys():
            created_date = parse(record["transactionDate"])
        else:
            created_date = None

        # Create the journal entry
        journal_entry = {
            "createdDate": created_date,
            "tranDate": created_date,
            "externalId": record["id"],
            "currency": currency_ref
        }

        return journal_entry
    

    def process_inbound_shipment(self, context, record):
        inbound_shipment = record
        inbound_shipment["internalId"] = record["id"]

        return inbound_shipment

    def process_item(self,context,record):
        ns = NetsuiteSoapClient(self.config)
        RecordRef = ns.search_client('RecordRef')
        RecordRefList = ns.search_client('RecordRefList')
        item_type = record.get('type', "").replace("-", "").lower()

        if item_type == 'noninventory':
            InventoryType = ns.search_client('NonInventorySaleItem')
            RecordRef = ns.search_client('RecordRef')

        else: 
            InventoryType = ns.search_client('InventoryItem')
         
        def get_account_by_name_or_id(x,accountName, id):
            if accountName:
                return x['acctName'] == accountName
            elif id:
                return x['internalId'] == id
            else:
                return False
        
        item = InventoryType(
            displayName = record.get('name'),
            createdDate = record.get('createdDate'),
            itemId = record.get('name'),
            upcCode = record.get('code'),
            isInactive = not record.get('active'),
            subsidiaryList=RecordRefList([RecordRef(internalId=record.get("subsidiary","1"))])
        )
    
        if record['type'] == 'Inventory':
            item.quantityOnHand = record.get('quantityOnHand')
        elif record['type'] == 'Non-Inventory':
            item.taxSchedule = RecordRef(internalId = record.get('taxSchedule'))

        if record.get('isBillItem'):
            cogsAccount = record.get('billItem')
            cost = cogsAccount.get('unitPrice')
            accountName = cogsAccount.get('accountName')
            id = cogsAccount.get('accountId')
            account = list(filter(lambda x: get_account_by_name_or_id(x,accountName,id), context['reference_data']['Accounts']))[0]
            item.costEstimate = cost
            item.cogsAccount = RecordRef(internalId = account['internalId'])
        
        if record.get('isInvoiceItem'):
            invoiceAccount = record.get('invoiceItem')

            try:
                price = invoiceAccount.get('unitPrice')
            except AttributeError:
                invoiceAccount = json.loads(invoiceAccount)
                price = invoiceAccount.get('unitPrice', None)
            
            item.taxSchedule = RecordRef(internalId=record.get('taxSchedule', 1))

            accountName = invoiceAccount.get('accountName')
            id = invoiceAccount.get('accountId')
            if accountName or id:
                try:
                    account = list(filter(lambda x: get_account_by_name_or_id(x, accountName, id), context['reference_data']['Accounts']))[0]
                    item.incomeAccount = RecordRef(internalId=account['internalId'])
                except IndexError:
                    self.logger.error(f"Account not found for {accountName} or {id}")
        return item
