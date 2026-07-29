package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.persistence.model.IncomingMessage
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.persistence.repository.MessageRepository
import kotlin.time.Instant
import kotlin.uuid.Uuid

class FakeMessageRepository : MessageRepository {
    private val messages = mutableMapOf<Uuid, IncomingMessage>()
    private var idCounter = 0L

    override suspend fun save(messageId: Uuid, receivedAt: Instant, result: ProcessingResult) {
        messages[messageId] = IncomingMessage(++idCounter, messageId, receivedAt, result)
    }

    override suspend fun findByMessageId(messageId: Uuid): IncomingMessage? = messages[messageId]
}
