package no.nav.helsemelding.inbound.persistence.repository

import no.nav.helsemelding.inbound.persistence.model.IncomingMessage
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.util.UuidTransformer
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.datetime.timestamp
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import kotlin.time.Instant
import kotlin.uuid.Uuid

object ProcessedMessages : Table("processed_messages") {
    val id = long("id").autoIncrement()
    override val primaryKey = PrimaryKey(id)
    val messageId = uuid("message_id").transform(UuidTransformer())
    val receivedAt = timestamp("received_at")
    val result = enumerationByName("result", 50, ProcessingResult::class)
}

interface MessageRepository {
    suspend fun save(messageId: Uuid, receivedAt: Instant, result: ProcessingResult)
    suspend fun findByMessageId(messageId: Uuid): IncomingMessage?
}

class ExposedMessageRepository(private val database: Database) : MessageRepository {
    override suspend fun save(messageId: Uuid, receivedAt: Instant, result: ProcessingResult): Unit =
        suspendTransaction(database) {
            ProcessedMessages.insert {
                it[ProcessedMessages.messageId] = messageId
                it[ProcessedMessages.receivedAt] = receivedAt
                it[ProcessedMessages.result] = result
            }
        }

    override suspend fun findByMessageId(messageId: Uuid): IncomingMessage? = suspendTransaction(database) {
        ProcessedMessages.selectAll()
            .where { ProcessedMessages.messageId eq messageId }
            .singleOrNull()
            ?.let {
                IncomingMessage(
                    id = it[ProcessedMessages.id],
                    messageId = it[ProcessedMessages.messageId],
                    receivedAt = it[ProcessedMessages.receivedAt],
                    result = it[ProcessedMessages.result]
                )
            }
    }
}
