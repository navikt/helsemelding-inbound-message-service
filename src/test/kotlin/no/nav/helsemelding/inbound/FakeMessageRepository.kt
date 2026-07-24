package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.persistence.model.IncomingMessage
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.persistence.repository.MessageRepository
import kotlin.time.Instant
import kotlin.uuid.Uuid

class FakeMessageRepository : MessageRepository {
    private val messages = mutableMapOf<Uuid, IncomingMessage>()

    override suspend fun save(id: Uuid, receivedAt: Instant, result: ProcessingResult) {
        messages[id] = IncomingMessage(id, receivedAt, result)
    }

    override suspend fun findById(id: Uuid): IncomingMessage? = messages[id]
}
