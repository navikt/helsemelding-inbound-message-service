package no.nav.helsemelding.inbound.persistence.model

import kotlin.time.Instant
import kotlin.uuid.Uuid

data class RefusedMessage(
    val notificationId: Uuid,
    val externalMessageId: Uuid?,
    val receiverHerId: Int,
    val description: String?,
    val receivedAt: Instant
)
