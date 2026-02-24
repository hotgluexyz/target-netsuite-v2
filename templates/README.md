# target-netsuite-v2 Configuration

This document describes the configuration options for the target-netsuite-v2 Singer target, which loads data into NetSuite via the SuiteTalk REST API.

---

## Authentication

#### `ns_consumer_key` (string, required)
The OAuth 1.0 consumer key (client ID) from your NetSuite integration record.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `ns_consumer_secret` (string, required)
The OAuth 1.0 consumer secret (client secret) from your NetSuite integration record.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `ns_token_key` (string, required)
The OAuth 1.0 token key (access token) for the user/role that will perform API requests.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `ns_token_secret` (string, required)
The OAuth 1.0 token secret (access token secret) for the user/role.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `ns_account` (string, required)
Your NetSuite account ID. Use the account ID as shown in NetSuite (e.g. with underscores; the target normalizes it for the API URL).
- **Example**: `"1234567"` or `"1234567_SB1"`

---

## Reference Data Snapshot

#### `snapshot_hours` (number, optional)
When set, reference data (subsidiaries, classifications, currencies, departments, locations, accounts, categories, tax codes) is cached to a file. If the file exists and was written less than this many hours ago, the target reuses it instead of fetching from the API. Use this to reduce API calls when running the target frequently.
- **Example**: `24` (reuse snapshot if it is less than 24 hours old)

#### `snapshot_dir` (string, optional)
Directory path used when **reading** the reference data snapshot file. Ignored if `snapshot_hours` is not set or if no snapshot exists yet.
- **Default**: `"snapshots"`
- **Example**: `"snapshots"` or `"/var/data/netsuite_snapshots"`

---

## Example: Minimal config (required options only)

```json
{
  "ns_consumer_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_consumer_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_token_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_token_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_account": "xxxxxxxxxxxx"
}
```

---

## Example: Full config (with optional options)

```json
{
  "ns_consumer_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_consumer_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_token_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_token_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "ns_account": "xxxxxxxxxxxx",
  "snapshot_hours": 24,
  "snapshot_dir": "snapshots"
}
```
