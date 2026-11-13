# Peer CLI protocol v1

A peer CLI accesses only its own existing local service. It never discovers other executables, starts a service, creates a target session, forks history, changes the model, or answers permissions and forms.

## Commands

```
<program> peer list [--cursor CURSOR]
<program> peer status --session ID
<program> peer queue --session ID --message TEXT [--resume] [--wait] [--timeout SECONDS]
<program> peer wait --receipt RECEIPT [--timeout SECONDS]
<program> peer read --session ID [--cursor CURSOR]
```

Each command writes one JSON object to stdout. Diagnostics go to stderr. Every response contains `version: 1` and `operation`. Errors contain `error: {code, message}` and exit nonzero. Completion, failure, timeout and blocking are structured results rather than transport errors.

`list` returns `sessions` and `nextCursor`; entries have `sessionID`, `title`, `directory`, `updatedAt` (epoch milliseconds), and `status`. The default page has up to 50 recent non-archived top-level sessions, including explicitly named empty sessions. Cursors are opaque and local to their operation and service.

`status` returns a snapshot without sending, loading, resuming, or interrupting work. Its fields are `sessionID`, `state`, `blocked`, `blockers`, `pendingCount`, `lastOutcome`, and `observedAt` (RFC3339). States are `idle`, `running`, `blocked`, `not_loaded`, and `archived`. Block reasons include `permission` and `input`; approval takes precedence in the summary. `lastOutcome` is null until available, then `succeeded`, `failed` or `interrupted`. An idle session may still have queued work; a slow model or tool is not a blocker. Missing sessions and unavailable services are errors, never idle snapshots.

`queue` returns `sessionID`, `receipt`, `result: queued` and a snapshot. Loaded sessions drain their queue at an idle boundary. `--resume` also restores an unloaded existing session in the same running service. A failed restore preserves the admission receipt and supplies `resumeError`. Retrying a queue after an uncertain transport failure can duplicate delivery: check history before resending.

`wait` returns the receipt, session ID, snapshot, messages and `result`. Results are `completed`, `failed`, `interrupted`, `blocked`, `not_loaded`, `archived`, `timeout` and `waiting_interrupted`. `queue --wait` uses the same wait implementation. The default timeout is 60 seconds; zero performs an immediate observation. A timeout or cancelled wait never cancels target work. If cancellation arrives before observation finishes, `status` may be null; the receipt remains available. A blocked wait does not answer or reject the pending question or approval. Continue waiting with the same receipt after human interaction. Receipt completion follows the admitted input, not the session's old idle state or its most recent unrelated response. Concurrent human input remains in the ordered history and may influence the reply.

`read` returns `sessionID`, `messages`, and `nextCursor`. Message records contain `id`, `role` and `text`. Additional native timeline records may appear. Follow `nextCursor` to read further output; do not transfer cursors between sessions or implementations. Wait returns reply content directly; read is optional for context and longer history.

Receipts are opaque versioned local-service locators, not credentials or authorization tokens. Do not log authentication data in them. Keep the service/data context that admitted the message when waiting after a restart.
