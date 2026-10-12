package no.nav.helsemelding.inbound.client

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import no.nav.helsemelding.inbound.model.ScanResult
import no.nav.helsemelding.inbound.model.ScanStatus
import no.nav.helsemelding.messageconverter.model.Attachment
import java.io.IOException
import java.util.Base64

class ClamAvClientSpec : StringSpec(
    {
        val content = "attachment content"
        val attachment = Attachment(
            description = "Sensitive description",
            contentType = "text/plain",
            contentBase64 = Base64.getEncoder().encodeToString(content.toByteArray())
        )

        "Should send decoded attachments as multipart and read results" {
            val engine = MockEngine { request ->
                request.method shouldBe HttpMethod.Post
                request.url.toString() shouldBe "http://clamav.nais-system.svc.cluster.local/api/v2/scan"

                val body = request.body.toByteArray().decodeToString()
                body shouldContain content
                body shouldNotContain attachment.contentBase64
                body shouldNotContain attachment.description
                body shouldContain "name=\"file0\""
                body shouldContain "name=\"file1\""
                body shouldContain "filename=\"file0\""
                body shouldContain "Content-Type: text/plain"

                val content = """
                    [
                        {
                            "filename": "file0",
                            "result": "OK",
                            "virus": "",
                            "error": ""
                        },
                        {
                            "filename": "file1",
                            "result": "FOUND",
                            "virus": "Shai-Hulud",
                            "error": ""
                        }
                    ]
                """.trimIndent()

                respond(
                    content,
                    headers = headersOf("Content-Type", "application/json")
                )
            }

            HttpClient(engine).use { httpClient ->
                val results = ClamAvClient(httpClient)
                    .scanAttachments(listOf(attachment, attachment))

                results shouldBe listOf(
                    ScanResult("file0", ScanStatus.OK),
                    ScanResult("file1", ScanStatus.FOUND, virus = "Shai-Hulud")
                )
            }
        }

        "Should handle scanning errors returned with HTTP 200" {
            val engine = MockEngine {
                val content = """
                    [
                        {
                            "filename": "file0",
                            "result": "ERROR",
                            "virus": "",
                            "error": "Scan failed"
                        }
                    ]
                """.trimIndent()

                respond(content)
            }
            HttpClient(engine).use { httpClient ->
                val results = ClamAvClient(httpClient).scanAttachments(listOf(attachment))

                results shouldBe listOf(ScanResult("file0", ScanStatus.ERROR, error = "Scan failed"))
            }
        }

        "Should fail on HTTP errors" {
            val engine = MockEngine {
                respond("Scan failed", HttpStatusCode.InternalServerError)
            }
            HttpClient(engine).use { httpClient ->
                shouldThrow<IOException> {
                    ClamAvClient(httpClient).scanAttachments(listOf(attachment))
                }
            }
        }

        "Should skip requests when there are no attachments" {
            val engine = MockEngine { error("No request expected") }
            HttpClient(engine).use { httpClient ->
                val results = ClamAvClient(httpClient).scanAttachments(emptyList())

                results shouldBe emptyList()
            }
        }

        "Should throw an error if attachments with invalid Base64" {
            val engine = MockEngine { error("No request expected") }
            HttpClient(engine).use { httpClient ->
                shouldThrow<IllegalArgumentException> {
                    ClamAvClient(httpClient)
                        .scanAttachments(listOf(attachment.copy(contentBase64 = "invalid!")))
                }
            }
        }
    }
)
