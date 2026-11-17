CREATE TABLE peer_context (id INTEGER PRIMARY KEY CHECK (id = 1), identity TEXT NOT NULL);
INSERT INTO peer_context VALUES (1, lower(hex(randomblob(16))));
CREATE TABLE peer_tasks (receipt TEXT PRIMARY KEY NOT NULL, payload_json TEXT NOT NULL);
