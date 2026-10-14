DROP INDEX idx_processed_messages_external_message_id;
CREATE INDEX idx_processed_messages_external_message_id ON processed_messages(external_message_id);

UPDATE processed_messages
SET result = 'MARKING_MESSAGE_AS_DOWNLOADED_FAILED'
WHERE result = 'MARKING_MESSAGE_AS_READ_FAILED';

CREATE TABLE refused_messages (
    notification_id UUID PRIMARY KEY,
    external_message_id UUID,
    receiver_her_id INTEGER NOT NULL,
    description TEXT,
    received_at TIMESTAMPTZ NOT NULL
);
