package no.nav.helsemelding.inbound.persistence.repository

import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import no.nav.helsemelding.inbound.container
import no.nav.helsemelding.inbound.database
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.uuid.Uuid

private fun Instant.truncatedToMicroseconds(): Instant =
    Instant.fromEpochSeconds(epochSeconds, nanosecondsOfSecond / 1_000 * 1_000)

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

                val messageId = Uuid.random()
                val receivedAt = Clock.System.now()

                repository.save(messageId, receivedAt, ProcessingResult.SUCCESS)

                val saved = repository.findByMessageId(messageId)
                saved shouldNotBe null
                saved!!.messageId shouldBe messageId
                saved.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "save should persist failed processing result" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val messageId = Uuid.random()

                repository.save(messageId, Clock.System.now(), ProcessingResult.PUBLISHING_TO_KAFKA_FAILED)

                val saved = repository.findByMessageId(messageId)
                saved!!.result shouldBe ProcessingResult.PUBLISHING_TO_KAFKA_FAILED
            }
        }

        "findByMessageId should return saved message" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val messageId = Uuid.random()
                val receivedAt = Clock.System.now().truncatedToMicroseconds()
                repository.save(messageId, receivedAt, ProcessingResult.SUCCESS)

                val found = repository.findByMessageId(messageId)
                found shouldNotBe null
                found!!.messageId shouldBe messageId
                found.receivedAt shouldBe receivedAt
                found.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "findByMessageId should return null for unknown messageId" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val result = repository.findByMessageId(Uuid.random())

                result shouldBe null
            }
        }
    }
)
