package no.nav.helsemelding.inbound.model

data class MessageProcessingOutcome(
    val result: ProcessingResult,
    val contentHash: String?
)
