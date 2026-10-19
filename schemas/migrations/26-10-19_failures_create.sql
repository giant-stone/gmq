CREATE TABLE IF NOT EXISTS failures (
  sequence INTEGER PRIMARY KEY,
  queue TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
  id TEXT NOT NULL,
  payload BLOB,
  created INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  error TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS failures_queue_id ON failures(queue, id, sequence);
