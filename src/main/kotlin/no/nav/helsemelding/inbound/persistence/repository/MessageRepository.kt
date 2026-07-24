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

object Messages : Table("messages") {
    val id = uuid("id").transform(UuidTransformer())
    override val primaryKey = PrimaryKey(id)
    val receivedAt = timestamp("received_at")
    val result = enumerationByName("result", 50, ProcessingResult::class)
}

interface MessageRepository {
    suspend fun save(id: Uuid, receivedAt: Instant, result: ProcessingResult)
    suspend fun findById(id: Uuid): IncomingMessage?
}

class ExposedMessageRepository(private val database: Database) : MessageRepository {
    override suspend fun save(id: Uuid, receivedAt: Instant, result: ProcessingResult): Unit =
        suspendTransaction(database) {
            Messages.insert {
                it[Messages.id] = id
                it[Messages.receivedAt] = receivedAt
                it[Messages.result] = result
            }
        }

    override suspend fun findById(id: Uuid): IncomingMessage? = suspendTransaction(database) {
        Messages.selectAll()
            .where { Messages.id eq id }
            .singleOrNull()
            ?.let { IncomingMessage(it[Messages.id], it[Messages.receivedAt], it[Messages.result]) }
    }
}
