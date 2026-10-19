CREATE TABLE IF NOT EXISTS messages (
  sequence INTEGER PRIMARY KEY,
  queue TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
  id TEXT NOT NULL,
  payload BLOB,
  created INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  state TEXT NOT NULL,
  UNIQUE (queue, id)
);
CREATE INDEX IF NOT EXISTS messages_queue_state ON messages(queue, state, sequence);
