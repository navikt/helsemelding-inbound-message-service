package no.nav.helsemelding.inbound.persistence.model

import kotlin.time.Instant
import kotlin.uuid.Uuid

data class ProcessedMessage(
    val id: Long,
    val externalMessageId: Uuid,
    val receivedAt: Instant,
    val result: ProcessingResult
)
