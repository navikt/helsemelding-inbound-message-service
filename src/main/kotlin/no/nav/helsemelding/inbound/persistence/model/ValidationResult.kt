package no.nav.helsemelding.inbound.persistence.model

import no.nav.helsemelding.inbound.metrics.ErrorTypeTag

enum class ValidationResult(val processingResult: ProcessingResult?, val errorTypeTag: ErrorTypeTag?) {
    OK(null, null),
    MISSING_MESSAGE_TEXT(ProcessingResult.MISSING_MESSAGE_TEXT, ErrorTypeTag.MISSING_MESSAGE_TEXT),
    VIRUS_DETECTED(ProcessingResult.VIRUS_DETECTED, ErrorTypeTag.VIRUS_DETECTED),
    VIRUS_SCAN_FAILED(ProcessingResult.VIRUS_SCAN_FAILED, ErrorTypeTag.VIRUS_SCAN_FAILED)
}
