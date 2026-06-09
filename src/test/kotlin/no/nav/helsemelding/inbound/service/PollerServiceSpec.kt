package no.nav.helsemelding.inbound.service

import arrow.core.Either.Left
import arrow.core.Either.Right
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.ediadapter.model.Metadata
import no.nav.helsemelding.inbound.FakeAttachmentService
import no.nav.helsemelding.inbound.FakeMessagePublisher
import no.nav.helsemelding.inbound.metrics.FakeMetrics
import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.model.SplitMessage
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.util.Base64
import kotlin.uuid.Uuid

const val FAGSYSTEM_HER_ID = 8142519

class PollerServiceSpec : StringSpec(
    {
        lateinit var ediAdapterClient: FakeEdiAdapterClient
        lateinit var publisher: FakeMessagePublisher
        lateinit var pollerService: PollerService
        lateinit var attachmentService: FakeAttachmentService

        beforeEach {
            ediAdapterClient = FakeEdiAdapterClient()
            publisher = FakeMessagePublisher()
            attachmentService = FakeAttachmentService()
            pollerService = PollerService(
                ediAdapterClient,
                publisher,
                attachmentService,
                FakeMetrics()
            )
        }

        "Apprec should be processed" {
            val messageId = Uuid.random()

            ediAdapterClient.givenMarkAsRead(Right(true))

            val message = Message(
                id = messageId,
                isAppRec = true,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe true
        }

        "Apprec should not be processed if EDI Adapter returns error" {
            val messageId = Uuid.random()

            val errorMessage500 = ErrorMessage(
                error = "Internal Server Error",
                errorCode = 500,
                requestId = Uuid.random().toString()
            )
            ediAdapterClient.givenMarkAsRead(Left(errorMessage500))

            val message = Message(
                id = messageId,
                isAppRec = true,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should be processed" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )
            ediAdapterClient.givenMarkAsRead(Right(true))
            ediAdapterClient.givenPostApprecResponse(
                Right(
                    Metadata(
                        id = Uuid.random(),
                        location = "http://example.com/apprec/${Uuid.random()}"
                    )
                )
            )

            attachmentService.givenSplitMessage(buildSplitMessage(xml))

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            val result = pollerService.processMessage(message)

            result shouldBe true

            publisher.publishedKey shouldBe messageId.toString()
            String(publisher.publishedPayload!!) shouldBe xml
        }

        "Incoming message should not be processed if retrieving business document fails" {
            val messageId = Uuid.random()

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Left(
                    ErrorMessage(
                        error = "Failed retrieving document",
                        errorCode = 404,
                        requestId = Uuid.random().toString()
                    )
                )
            )

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if parsing business document fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )

            attachmentService.givenSplitMessage(null)

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if saving attachments fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )

            attachmentService.givenSplitMessage(buildSplitMessage(xml))
            attachmentService.givenIsAttachmentsSaved(false)

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if publishing to Kafka fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )

            attachmentService.givenSplitMessage(buildSplitMessage(xml))

            publisher.givenPublishingResult(Result.failure(RuntimeException("Kafka unavailable")))

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if marking as read fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )

            val errorMessage500 = ErrorMessage(
                error = "Internal Server Error",
                errorCode = 500,
                requestId = Uuid.random().toString()
            )
            ediAdapterClient.givenMarkAsRead(Left(errorMessage500))

            attachmentService.givenSplitMessage(buildSplitMessage(xml))

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if sending apprec fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"
                    )
                )
            )
            ediAdapterClient.givenMarkAsRead(Right(true))
            ediAdapterClient.givenPostApprecResponse(
                Left(
                    ErrorMessage(
                        error = "Sending apprec failed",
                        errorCode = 500,
                        requestId = Uuid.random().toString()
                    )
                )
            )

            attachmentService.givenSplitMessage(buildSplitMessage(xml))

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            val result = pollerService.processMessage(message)

            result shouldBe false
        }
    }

)

fun buildSuccessfulPublishingResult(): Result<RecordMetadata> {
    val record = RecordMetadata(
        TopicPartition("test", 0),
        0L,
        0,
        System.currentTimeMillis(),
        0,
        0
    )
    return Result.success(record)
}

fun buildSplitMessage(xml: String) = SplitMessage(
    messageWithoutAttachmentXml = xml,
    attachments = listOf(
        Attachment(
            description = "Test attachment",
            contentType = "application/pdf",
            contentBase64 = "VGhpcyBpcyBhIHRlc3QgYXR0YWNobWVudA=="
        )
    )
)

fun readFileToString(path: String): String {
    return PollerServiceSpec::class.java.classLoader.getResource(path)!!.readText()
}
