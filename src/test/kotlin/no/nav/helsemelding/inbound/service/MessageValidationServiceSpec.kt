package no.nav.helsemelding.inbound.service

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.CancellationException
import no.nav.helsemelding.inbound.FakeMessageRepository
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.persistence.model.ValidationResult
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import java.io.IOException
import kotlin.time.Clock
import kotlin.uuid.Uuid

class MessageValidationServiceSpec : StringSpec(
    {
        val attachment = Attachment("Test attachment", "text/plain", "dGVzdA==")
        val message = SplitMessage("<MsgHead/>", listOf(attachment, attachment))
        val messageId = Uuid.random()
        val messageRepository = FakeMessageRepository()

        "Should accept attachments when all scanning results are OK" {
            val engine = MockEngine {
                respond(
                    """
                    [
                        {"filename": "file0", "result": "OK", "virus": "", "error": ""},
                        {"filename": "file1", "result": "OK", "virus": "", "error": ""}
                    ]
                    """.trimIndent()
                )
            }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                validationService.validate(message, messageId, null) shouldBe ValidationResult.OK
            }
        }

        "Should reject attachments if any scanning result is not OK" {
            listOf("FOUND", "ERROR").forEach { status ->
                val engine = MockEngine {
                    respond(
                        """
                        [
                            {"filename": "file0", "result": "OK"},
                            {"filename": "file1", "result": "$status"}
                        ]
                        """.trimIndent()
                    )
                }

                HttpClient(engine).use { client ->
                    val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                    validationService.validate(message, messageId, null) shouldBe ValidationResult.VIRUS_DETECTED
                }
            }
        }

        "Should reject empty, malformed or unexpected scanning results" {
            listOf(
                "[]" to ValidationResult.VIRUS_SCAN_FAILED,
                "invalid JSON" to ValidationResult.VIRUS_SCAN_FAILED,
                """[{"filename": "file0", "result": "UNKNOWN"}]""" to ValidationResult.VIRUS_SCAN_FAILED,
                """[{"filename": "file0"}]""" to ValidationResult.VIRUS_SCAN_FAILED
            ).forEach { (response, expected) ->
                val engine = MockEngine { respond(response) }

                HttpClient(engine).use { client ->
                    val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                    validationService.validate(message, messageId, null) shouldBe expected
                }
            }
        }

        "Should reject attachments on HTTP errors" {
            val engine = MockEngine { respond("Failed", HttpStatusCode.InternalServerError) }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                validationService.validate(message, messageId, null) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should reject attachments on network errors" {
            val engine = MockEngine { throw IOException("Connection failed") }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                validationService.validate(message, messageId, null) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should reject invalid Base64 without sending a request" {
            val engine = MockEngine { error("No request expected") }

            HttpClient(engine).use { client ->
                val invalidMessage = message.copy(
                    attachments = listOf(attachment.copy(contentBase64 = "invalid!"))
                )

                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                validationService.validate(invalidMessage, messageId, null) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should accept messages without attachments without sending a request" {
            val engine = MockEngine { error("No request expected") }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                validationService.validate(message.copy(attachments = emptyList()), messageId, null) shouldBe ValidationResult.OK
            }
        }

        "Should propagate coroutine cancellation" {
            val engine = MockEngine { throw CancellationException("Cancelled") }

            HttpClient(engine).use { client ->
                shouldThrow<CancellationException> {
                    val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                    validationService.validate(message, messageId, null)
                }
            }
        }

        "Should reject duplicate content" {
            val contentHash = "a".repeat(64)
            messageRepository.save(
                Uuid.random(),
                Clock.System.now(),
                ProcessingResult.SUCCESS,
                contentHash
            )
            val engine = MockEngine { error("Duplicate messages must not be scanned") }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client), messageRepository)
                val validationResult = validationService.validate(message, messageId, contentHash)
                validationResult shouldBe ValidationResult.DUPLICATE_MESSAGE_DETECTED
            }
        }
    }
)
