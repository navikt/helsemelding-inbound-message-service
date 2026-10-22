package no.nav.helsemelding.inbound.model

import no.nav.helsemelding.inbound.metrics.ErrorTypeTag

enum class ValidationResult(val processingResult: ProcessingResult?, val errorTypeTag: ErrorTypeTag?) {
    OK(null, null),
    VIRUS_DETECTED(ProcessingResult.VIRUS_DETECTED, ErrorTypeTag.VIRUS_DETECTED),
    VIRUS_SCAN_FAILED(ProcessingResult.VIRUS_SCAN_FAILED, ErrorTypeTag.VIRUS_SCAN_FAILED)
}
