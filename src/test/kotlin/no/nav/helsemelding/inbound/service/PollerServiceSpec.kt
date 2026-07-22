package no.nav.helsemelding.inbound.service

import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.right
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.ediadapter.model.Metadata
import no.nav.helsemelding.inbound.FakeAttachmentService
import no.nav.helsemelding.inbound.FakeMessagePublisher
import no.nav.helsemelding.inbound.metrics.FakeMetrics
import no.nav.helsemelding.message.converter.MsgHeadMessageConverter
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.util.Base64
import kotlin.uuid.Uuid

private const val FAGSYSTEM_HER_ID = 8142519

class PollerServiceSpec : StringSpec(
    {
        lateinit var ediAdapterClient: FakeEdiAdapterClient
        lateinit var publisher: FakeMessagePublisher
        lateinit var pollerService: PollerService
        lateinit var attachmentService: FakeAttachmentService
        lateinit var messageConverter: MsgHeadMessageConverter

        beforeEach {
            ediAdapterClient = FakeEdiAdapterClient()
            publisher = FakeMessagePublisher()
            attachmentService = FakeAttachmentService()
            messageConverter = MsgHeadMessageConverter()
            pollerService = PollerService(
                ediAdapterClient,
                publisher,
                attachmentService,
                messageConverter,
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

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            val result = pollerService.processMessage(message)

            result shouldBe true

            publisher.publishedKey shouldBe messageId.toString()
            String(publisher.publishedPayload!!) shouldBe messageConverter.expectedPayload(xml)
        }

        "Incoming message with attachments should save attachments and publish message without attachments" {
            val messageId = Uuid.random()

            val xml = readFileToString("message_with_attachments.xml")
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

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            val result = pollerService.processMessage(message)

            result shouldBe true

            attachmentService.saveAttachmentsCallCount shouldBe 1
            attachmentService.savedMessageId shouldBe messageId

            val savedAttachments = attachmentService.savedAttachments!!
            savedAttachments.map { it.description } shouldBe listOf(
                "Testvedlegg 1",
                "Testvedlegg 2",
                "Testvedlegg 3"
            )
            savedAttachments.map { it.contentType } shouldBe listOf(
                "application/pdf",
                "application/pdf",
                "application/pdf"
            )
            savedAttachments.all { it.contentBase64.isNotBlank() } shouldBe true

            val publishedPayload = String(publisher.publishedPayload!!)
            publishedPayload shouldBe messageConverter.expectedPayload(xml)
            publishedPayload.contains("Testvedlegg 1") shouldBe false
            publishedPayload.contains("Testvedlegg 2") shouldBe false
            publishedPayload.contains("Testvedlegg 3") shouldBe false
            publishedPayload.contains("Base64Container") shouldBe false
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

            val xml = "<MsgHead>"
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

            val message = Message(
                id = messageId,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }

        "Incoming message should not be processed if saving attachments fails" {
            val messageId = Uuid.random()

            val xml = readFileToString("message_with_attachments.xml")
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

            attachmentService.givenSaveAttachmentsEither(
                RuntimeException("Saving attachments failed").left()
            )

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

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

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

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

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

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

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

fun readFileToString(path: String): String {
    return PollerServiceSpec::class.java.classLoader.getResource(path)!!.readText()
}

private fun MsgHeadMessageConverter.expectedPayload(xml: String): String =
    splitAttachments(xml)
        .getOrElse { error("Failed to split test XML: $it") }
        .messageWithoutAttachmentsXml
