# Peer CLI protocol v2

The authoritative shared contract is the `agent-peer-protocol` AFL project: `afl/core/peer/{lib,types,protocol,lifecycle}.afl`, with product identity edges, native implementation associations and acceptance specifications. Maintain shared behavior there before changing either native implementation.

```text
<product> peer list [--cur-dir | --dir PATH] [--cursor CURSOR]
<product> peer status ID
<product> peer read ID [--cursor CURSOR]
<product> peer queue ID --message TEXT --from PRODUCT:SESSION_ID [--resume]
<product> peer reply --receipt RECEIPT --message TEXT [--final]
```

The user first obtains the target's full Session ID using `/session-id` and explicitly selects that existing session. The sending agent provides its own existing identity through `--from`. No operation starts a service or creates/forks a session. Query commands are passive.

Queue acknowledges durable native admission, independently of execution. The target can work across multiple turns and explicitly send progress, then a final reply. Ordinary output and idle do not complete a task. Reply first saves the message, then queues it back to the original sender without steering, interrupting or implicitly restoring an unloaded session. Failed return delivery remains pending; repeating an identical pending reply reuses its fixed message ID. Final saves atomically close the task while allowing pending delivery retries.

Receipts contain 128 random bits encoded as 22 Base64URL characters. Durable routes bind the exact product, data context and session. Records do not automatically expire, and there is no background retry or cleanup. Version 1 receipts require manual reconciliation; `wait`, `queue --wait` and `--timeout` are removed.

Each command writes one version 2 JSON object and newline to stdout. Errors exit nonzero. `docs/fixtures/peer-v1.json` is historical, not the current contract. Native tests cover persistence, admission deduplication, explicit replies, concurrency and isolated service integration; follow the shared AFL test specifications when extending coverage.
