package no.nav.helsemelding.inbound.metrics

enum class ErrorTypeTag(val value: String) {
    RETRIEVING_BUSINESS_DOCUMENT_FAILED("retrieving_business_document_failed"),
    PUBLISHING_TO_KAFKA_FAILED("publishing_to_kafka_failed"),
    MARKING_MESSAGE_AS_READ_FAILED("marking_message_as_read_failed"),
    SENDING_APPREC_FAILED("sending_apprec_failed")
}
