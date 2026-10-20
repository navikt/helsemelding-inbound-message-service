package no.nav.helsemelding.inbound.persistence.repository

import arrow.fx.coroutines.resourceScope
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import no.nav.helsemelding.inbound.container
import no.nav.helsemelding.inbound.database
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import java.sql.SQLException
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.uuid.Uuid

private fun Instant.truncatedToMicroseconds(): Instant =
    Instant.fromEpochSeconds(epochSeconds, nanosecondsOfSecond / 1000 * 1000)

class MessageRepositorySpec : StringSpec(
    {
        val postgresContainer = container()

        beforeSpec {
            postgresContainer.start()
        }

        "save should persist processing result" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val externalMessageId = Uuid.random()
                val receivedAt = Clock.System.now()

                repository.save(externalMessageId, receivedAt, ProcessingResult.SUCCESS)

                val saved = repository.findByExternalMessageId(externalMessageId)
                saved shouldNotBe null
                saved!!.externalMessageId shouldBe externalMessageId
                saved.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "save should persist failed processing result" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val externalMessageId = Uuid.random()

                repository.save(externalMessageId, Clock.System.now(), ProcessingResult.PUBLISHING_TO_KAFKA_FAILED)

                val saved = repository.findByExternalMessageId(externalMessageId)
                saved!!.result shouldBe ProcessingResult.PUBLISHING_TO_KAFKA_FAILED
            }
        }

        "findByExternalMessageId should return saved message" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val externalMessageId = Uuid.random()
                val receivedAt = Clock.System.now().truncatedToMicroseconds()
                repository.save(externalMessageId, receivedAt, ProcessingResult.SUCCESS)

                val found = repository.findByExternalMessageId(externalMessageId)
                found shouldNotBe null
                found!!.externalMessageId shouldBe externalMessageId
                found.receivedAt shouldBe receivedAt
                found.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "findByExternalMessageId should return null for unknown externalMessageId" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val result = repository.findByExternalMessageId(Uuid.random())

                result shouldBe null
            }
        }

        "external message ID remains unique and the original result is preserved" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)
                val id = Uuid.random()
                repository.save(id, Clock.System.now(), ProcessingResult.SENDING_APPREC_FAILED)
                val failure = shouldThrow<SQLException> {
                    repository.save(id, Clock.System.now(), ProcessingResult.SUCCESS)
                }
                failure.sqlState shouldBe "23505"
                repository.findByExternalMessageId(id)!!.result shouldBe ProcessingResult.SENDING_APPREC_FAILED
                suspendTransaction(database) {
                    ProcessedMessages.selectAll().where { ProcessedMessages.externalMessageId eq id }.count() shouldBe 1L
                }
            }
        }
    }
)
