package no.nav.helsemelding.inbound.metrics

enum class ErrorTypeTag(val value: String) {
    MISSING_EXTERNAL_MESSAGE_ID("missing_external_message_id"),
    RETRIEVING_BUSINESS_DOCUMENT_FAILED("retrieving_business_document_failed"),
    PUBLISHING_TO_KAFKA_FAILED("publishing_to_kafka_failed"),
    MARKING_MESSAGE_AS_DOWNLOADED_FAILED("marking_message_as_downloaded_failed"),
    DECODING_BUSINESS_DOCUMENT_FAILED("decoding_business_document_failed"),
    DELETING_NOTIFICATION_FAILED("deleting_notification_failed"),
    SENDING_APPREC_FAILED("sending_apprec_failed"),
    SPLITTING_MESSAGE_FAILED("splitting_message_failed"),
    SAVING_ATTACHMENTS_FAILED("saving_attachments_failed"),
    EXTRACTING_MESSAGE_ID_FAILED("extracting_message_id_failed"),
    MISSING_MESSAGE_TEXT("missing_message_text"),
    VIRUS_DETECTED("validating_virus_detected"),
    VIRUS_SCAN_FAILED("validating_virus_scan_failed")
}
