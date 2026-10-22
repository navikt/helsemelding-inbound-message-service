ALTER TABLE processed_messages ADD COLUMN content_hash VARCHAR(64);

CREATE INDEX idx_processed_messages_content_hash ON processed_messages(content_hash);
