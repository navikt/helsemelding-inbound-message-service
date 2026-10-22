package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.MessageProcessingOutcome
import no.nav.helsemelding.inbound.model.ProcessedMessage
import no.nav.helsemelding.inbound.repository.MessageRepository
import kotlin.time.Instant
import kotlin.uuid.Uuid

class FakeMessageRepository : MessageRepository {
    private val messages = mutableMapOf<Uuid, ProcessedMessage>()
    private var idCounter = 0L

    override suspend fun save(
        externalMessageId: Uuid,
        receivedAt: Instant,
        processingOutcome: MessageProcessingOutcome
    ) {
        check(externalMessageId !in messages) { "Message $externalMessageId is already registered" }
        messages[externalMessageId] = ProcessedMessage(
            ++idCounter,
            externalMessageId,
            receivedAt,
            processingOutcome.result,
            processingOutcome.contentHash
        )
    }

    override suspend fun findByExternalMessageId(externalMessageId: Uuid): ProcessedMessage? =
        messages[externalMessageId]

    override suspend fun findByContentHash(contentHash: String): ProcessedMessage? =
        messages.values.firstOrNull { it.contentHash == contentHash }
}
