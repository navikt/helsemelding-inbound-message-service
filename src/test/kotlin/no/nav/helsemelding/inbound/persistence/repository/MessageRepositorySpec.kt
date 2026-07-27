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

                val id = Uuid.random()
                val receivedAt = Clock.System.now()

                repository.save(id, receivedAt, ProcessingResult.SUCCESS)

                val saved = repository.findById(id)
                saved shouldNotBe null
                saved!!.id shouldBe id
                saved.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "save should persist failed processing result" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val id = Uuid.random()

                repository.save(id, Clock.System.now(), ProcessingResult.PUBLISHING_TO_KAFKA_FAILED)

                val saved = repository.findById(id)
                saved!!.result shouldBe ProcessingResult.PUBLISHING_TO_KAFKA_FAILED
            }
        }

        "findById should return saved message" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val id = Uuid.random()
                val receivedAt = Clock.System.now().truncatedToMicroseconds()
                repository.save(id, receivedAt, ProcessingResult.SUCCESS)

                val found = repository.findById(id)
                found shouldNotBe null
                found!!.id shouldBe id
                found.receivedAt shouldBe receivedAt
                found.result shouldBe ProcessingResult.SUCCESS
            }
        }

        "findById should return null for unknown id" {
            resourceScope {
                val database = database(postgresContainer.jdbcUrl)
                val repository = ExposedMessageRepository(database)

                val result = repository.findById(Uuid.random())

                result shouldBe null
            }
        }
    }
)
