ALTER TABLE processed_messages
    RENAME COLUMN message_id TO external_message_id;

ALTER INDEX idx_processed_messages_message_id
    RENAME TO idx_processed_messages_external_message_id;
