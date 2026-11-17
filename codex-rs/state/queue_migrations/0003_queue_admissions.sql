CREATE TABLE queue_admissions (
    client_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

INSERT OR IGNORE INTO queue_admissions (client_id, thread_id, item_id, payload_json)
SELECT json_extract(payload_json, '$.UserInput.client_id'), thread_id, id, payload_json
FROM queued_items
WHERE json_valid(payload_json)
  AND json_type(payload_json, '$.UserInput.client_id') = 'text';
