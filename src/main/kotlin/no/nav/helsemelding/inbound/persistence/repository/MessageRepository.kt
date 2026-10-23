package no.nav.helsemelding.inbound.persistence.repository

import no.nav.helsemelding.inbound.persistence.model.MessageProcessingOutcome
import no.nav.helsemelding.inbound.persistence.model.ProcessedMessage
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
    val externalMessageId = uuid("external_message_id").transform(UuidTransformer())
    val receivedAt = timestamp("received_at")
    val result = enumerationByName("result", 50, ProcessingResult::class)
    val contentHash = varchar("content_hash", 64).nullable()
}

interface MessageRepository {
    suspend fun save(
        externalMessageId: Uuid,
        receivedAt: Instant,
        processingOutcome: MessageProcessingOutcome
    )
    suspend fun findByExternalMessageId(externalMessageId: Uuid): ProcessedMessage?
    suspend fun findByContentHash(contentHash: String): ProcessedMessage?
}

class ExposedMessageRepository(private val database: Database) : MessageRepository {
    override suspend fun save(
        externalMessageId: Uuid,
        receivedAt: Instant,
        processingOutcome: MessageProcessingOutcome
    ): Unit =
        suspendTransaction(database) {
            ProcessedMessages.insert {
                it[ProcessedMessages.externalMessageId] = externalMessageId
                it[ProcessedMessages.receivedAt] = receivedAt
                it[ProcessedMessages.result] = processingOutcome.result
                it[ProcessedMessages.contentHash] = processingOutcome.contentHash
            }
        }

    override suspend fun findByExternalMessageId(externalMessageId: Uuid): ProcessedMessage? = suspendTransaction(database) {
        ProcessedMessages.selectAll()
            .where { ProcessedMessages.externalMessageId eq externalMessageId }
            .singleOrNull()
            ?.let {
                ProcessedMessage(
                    id = it[ProcessedMessages.id],
                    externalMessageId = it[ProcessedMessages.externalMessageId],
                    receivedAt = it[ProcessedMessages.receivedAt],
                    result = it[ProcessedMessages.result],
                    contentHash = it[ProcessedMessages.contentHash]
                )
            }
    }

    override suspend fun findByContentHash(contentHash: String): ProcessedMessage? = suspendTransaction(database) {
        ProcessedMessages.selectAll()
            .where { ProcessedMessages.contentHash eq contentHash }
            .limit(1)
            .singleOrNull()
            ?.let {
                ProcessedMessage(
                    id = it[ProcessedMessages.id],
                    externalMessageId = it[ProcessedMessages.externalMessageId],
                    receivedAt = it[ProcessedMessages.receivedAt],
                    result = it[ProcessedMessages.result],
                    contentHash = it[ProcessedMessages.contentHash]
                )
            }
    }
}
