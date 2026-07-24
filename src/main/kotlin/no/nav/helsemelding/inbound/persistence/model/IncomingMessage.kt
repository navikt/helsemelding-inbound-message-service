package no.nav.helsemelding.inbound.persistence.model

import kotlin.time.Instant
import kotlin.uuid.Uuid

data class IncomingMessage(
    val id: Uuid,
    val receivedAt: Instant,
    val result: ProcessingResult
)
