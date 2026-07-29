ALTER TABLE processed_messages ADD COLUMN message_id UUID;
UPDATE processed_messages SET message_id = id;
ALTER TABLE processed_messages ALTER COLUMN message_id SET NOT NULL;

ALTER TABLE processed_messages DROP CONSTRAINT messages_pkey;
ALTER TABLE processed_messages DROP COLUMN id;

ALTER TABLE processed_messages ADD COLUMN id BIGSERIAL PRIMARY KEY;

CREATE UNIQUE INDEX idx_processed_messages_message_id ON processed_messages(message_id);
