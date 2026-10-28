package no.nav.helsemelding.inbound.service

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.CancellationException
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.persistence.model.ValidationResult
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import java.io.IOException
import kotlin.uuid.Uuid

class MessageValidationServiceSpec : StringSpec(
    {
        val attachment = Attachment("Test attachment", "text/plain", "dGVzdA==")
        val message = SplitMessage("<MsgHead/>", listOf(attachment, attachment))
        val messageId = Uuid.random()

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
                val validationService = MessageValidationService(ClamAvClient(client))
                validationService.validate(message, messageId) shouldBe ValidationResult.OK
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
                    val validationService = MessageValidationService(ClamAvClient(client))
                    validationService.validate(message, messageId) shouldBe ValidationResult.VIRUS_DETECTED
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
                    val validationService = MessageValidationService(ClamAvClient(client))
                    validationService.validate(message, messageId) shouldBe expected
                }
            }
        }

        "Should reject attachments on HTTP errors" {
            val engine = MockEngine { respond("Failed", HttpStatusCode.InternalServerError) }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client))
                validationService.validate(message, messageId) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should reject attachments on network errors" {
            val engine = MockEngine { throw IOException("Connection failed") }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client))
                validationService.validate(message, messageId) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should reject invalid Base64 without sending a request" {
            val engine = MockEngine { error("No request expected") }

            HttpClient(engine).use { client ->
                val invalidMessage = message.copy(
                    attachments = listOf(attachment.copy(contentBase64 = "invalid!"))
                )

                val validationService = MessageValidationService(ClamAvClient(client))
                validationService.validate(invalidMessage, messageId) shouldBe ValidationResult.VIRUS_SCAN_FAILED
            }
        }

        "Should accept messages without attachments without sending a request" {
            val engine = MockEngine { error("No request expected") }

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client))
                validationService.validate(message.copy(attachments = emptyList()), messageId) shouldBe ValidationResult.OK
            }
        }

        "Should reject dialog messages that require text when the text is missing or empty" {
            val engine = MockEngine { error("No request expected") }
            val messages = listOf(
                "DIALOG_NOTAT" to ("8128" to "1"),
                "DIALOG_NOTAT" to ("8128" to "2"),
                "DIALOG_SVAR" to ("9069" to "5")
            )

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client))

                messages.forEach { (messageType, topic) ->
                    val missingTextMessage = SplitMessage(
                        incomingMessageXml(messageType, topic.first, topic.second, null),
                        emptyList()
                    )
                    val emptyTextMessage = SplitMessage(
                        incomingMessageXml(messageType, topic.first, topic.second, ""),
                        emptyList()
                    )

                    validationService.validate(missingTextMessage, messageId) shouldBe
                        ValidationResult.MISSING_MESSAGE_TEXT
                    validationService.validate(emptyTextMessage, messageId) shouldBe
                        ValidationResult.MISSING_MESSAGE_TEXT
                }
            }
        }

        "Should accept non-empty text and message types that do not require it" {
            val engine = MockEngine { error("No request expected") }
            val validMessages = listOf(
                incomingMessageXml("DIALOG_NOTAT", "8128", "1", "Valid message"),
                incomingMessageXml("DIALOG_SVAR", "9069", "5", "Valid response"),
                incomingMessageXml("DIALOG_SVAR", "8126", "1", null)
            )

            HttpClient(engine).use { client ->
                val validationService = MessageValidationService(ClamAvClient(client))

                validMessages.forEach { xml ->
                    validationService.validate(SplitMessage(xml, emptyList()), messageId) shouldBe
                        ValidationResult.OK
                }
            }
        }

        "Should propagate coroutine cancellation" {
            val engine = MockEngine { throw CancellationException("Cancelled") }

            HttpClient(engine).use { client ->
                shouldThrow<CancellationException> {
                    val validationService = MessageValidationService(ClamAvClient(client))
                    validationService.validate(message, messageId)
                }
            }
        }
    }
)

private fun incomingMessageXml(
    messageType: String,
    codeSystem: String,
    code: String,
    messageText: String?
): String {
    val textElement = messageText?.let { "<TekstNotatInnhold>$it</TekstNotatInnhold>" }.orEmpty()

    return """
        <MsgHead xmlns="http://www.kith.no/xmlstds/msghead/2006-05-24"
                 xmlns:fk1="http://www.kith.no/xmlstds/felleskomponent1">
            <MsgInfo>
                <Type V="$messageType"/>
                <GenDate>2026-03-09T16:15:14.884979286+01:00</GenDate>
                <MsgId>1e9b0104-fd84-4d29-824f-154d7f501262</MsgId>
                <Sender>
                    <Organisation>
                        <OrganisationName>Test Clinic</OrganisationName>
                        <Ident>
                            <Id>00000000000</Id>
                            <TypeId V="FNR"/>
                        </Ident>
                        <HealthcareProfessional>
                            <Ident>
                                <fk1:Id>00000000000</fk1:Id>
                                <fk1:TypeId V="FNR"/>
                            </Ident>
                        </HealthcareProfessional>
                    </Organisation>
                </Sender>
                <Patient>
                    <Ident>
                        <Id>00000000000</Id>
                        <TypeId V="FNR"/>
                    </Ident>
                </Patient>
            </MsgInfo>
            <Document>
                <RefDoc>
                    <IssueDate V="2026-03-09T16:15:14"/>
                    <MsgType V="XML"/>
                    <Content>
                        <Dialogmelding xmlns="http://www.kith.no/xmlstds/dialog/2006-10-11">
                            <Notat>
                                <TemaKodet V="$code" S="2.16.578.1.12.4.1.1.$codeSystem"/>
                                $textElement
                                <DokIdNotat>test-document-id</DokIdNotat>
                            </Notat>
                        </Dialogmelding>
                    </Content>
                </RefDoc>
            </Document>
        </MsgHead>
    """.trimIndent()
}
