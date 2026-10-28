package no.nav.helsemelding.inbound.persistence.model

data class MessageProcessingOutcome(
    val result: ProcessingResult,
    val contentHash: String?
)
