package no.nav.helsemelding.inbound.service

import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.left
import arrow.core.right
import arrow.fx.coroutines.resourceScope
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import no.nav.helsemelding.ediadapter.client.EdiAdapterError
import no.nav.helsemelding.ediadapter.model.common.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.v3.NotificationType
import no.nav.helsemelding.ediadapter.model.v3.PostApprecResponse
import no.nav.helsemelding.ediadapter.model.v3.UnreadNotification
import no.nav.helsemelding.inbound.FakeAttachmentService
import no.nav.helsemelding.inbound.FakeEdiAdapterClient
import no.nav.helsemelding.inbound.FakeMessagePublisher
import no.nav.helsemelding.inbound.FakeMessageRepository
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.metrics.FakeMetrics
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.persistence.model.RefusedMessage
import no.nav.helsemelding.inbound.persistence.repository.MessageRepository
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.util.Base64
import kotlin.time.Instant
import kotlin.uuid.Uuid
import no.nav.helsemelding.inbound.util.coroutineScope as resourceCoroutineScope

private const val FAGSYSTEM_HER_ID = 8142519

class NotificationServiceSpec : StringSpec(
    {
        lateinit var ediAdapterClient: FakeEdiAdapterClient
        lateinit var publisher: FakeMessagePublisher
        lateinit var notificationService: NotificationService
        lateinit var attachmentService: FakeAttachmentService
        lateinit var messageConverter: MsgHeadMessageConverter
        lateinit var messageRepository: FakeMessageRepository

        beforeEach {
            ediAdapterClient = FakeEdiAdapterClient()
            publisher = FakeMessagePublisher()
            attachmentService = FakeAttachmentService()
            messageConverter = MsgHeadMessageConverter()
            messageRepository = FakeMessageRepository()
            notificationService = NotificationService(
                ediAdapterClient,
                publisher,
                attachmentService,
                messageConverter,
                FakeMetrics(),
                messageRepository
            )
        }

        fun givenSuccessfulDocument() {
            val xml = readFileToString("message/incomingDialogMessage.xml")
            ediAdapterClient.givenGetBusinessDocumentResponse(
                GetBusinessDocumentResponse(Base64.getEncoder().encodeToString(xml.toByteArray()), "application/xml", "base64").right()
            )
            publisher.givenPublishingResult(buildSuccessfulPublishingResult())
        }

        "Apprec status should be ignored" {
            val externalMessageId = Uuid.random()

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.MESSAGE_APPREC_INFO_UPDATED,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            notificationService.processNotification(message)
            messageRepository.findByExternalMessageId(externalMessageId) shouldBe null
            publisher.publishedPayload shouldBe null
            attachmentService.saveAttachmentsCallCount shouldBe 0
        }

        "Incoming message should be processed" {
            val externalMessageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
            val xmlMessageId = messageConverter.extractMessageId(xml).shouldBeRight()
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
            ediAdapterClient.givenMarkAsDownloaded(Right(Unit))
            ediAdapterClient.givenPostApprecResponse(
                Right(
                    PostApprecResponse(Uuid.random())
                )
            )

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            notificationService.processNotification(message)

            publisher.publishedKey shouldBe xmlMessageId.toString()
            String(publisher.publishedPayload!!) shouldBe messageConverter.expectedPayload(xml)
            publisher.publishedAttachmentCount shouldBe 0

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.SUCCESS
        }

        "Incoming message with attachments should save attachments and publish message without attachments" {
            val externalMessageId = Uuid.random()

            val xml = readFileToString("message_with_attachments.xml")
            val xmlMessageId = messageConverter.extractMessageId(xml).shouldBeRight()
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
            ediAdapterClient.givenMarkAsDownloaded(Right(Unit))
            ediAdapterClient.givenPostApprecResponse(
                Right(
                    PostApprecResponse(Uuid.random())
                )
            )

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            notificationService.processNotification(message)

            attachmentService.saveAttachmentsCallCount shouldBe 1
            attachmentService.savedMessageId shouldBe xmlMessageId

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

            publisher.publishedKey shouldBe xmlMessageId.toString()
            publisher.publishedAttachmentCount shouldBe 3
        }

        "Incoming message should not be processed if retrieving business document fails" {
            val externalMessageId = Uuid.random()

            ediAdapterClient.givenGetBusinessDocumentResponse(
                Left(
                    EdiAdapterError.Api(404)
                )
            )

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.RETRIEVING_BUSINESS_DOCUMENT_FAILED
        }

        "Incoming message should not be processed if splitting the business document fails" {
            val externalMessageId = Uuid.random()

            val xml = readFileToString("message_with_attachments.xml")
                .replace("Base64Container", "InvalidAttachmentContent")
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

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.SPLITTING_MESSAGE_FAILED
        }

        "Incoming message should not be processed if extracting message ID fails" {
            val externalMessageId = Uuid.random()

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

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.EXTRACTING_MESSAGE_ID_FAILED
        }

        "Incoming message should not be processed if message ID is invalid" {
            val externalMessageId = Uuid.random()

            val xml = readFileToString("message/incomingDialogMessage.xml")
                .replace("1e9b0104-fd84-4d29-824f-154d7f501262", "invalid-message-id")
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

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.EXTRACTING_MESSAGE_ID_FAILED
        }

        "Incoming message should not be processed if saving attachments fails" {
            val externalMessageId = Uuid.random()

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

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.SAVING_ATTACHMENTS_FAILED
        }

        "Incoming message should not be processed if publishing to Kafka fails" {
            val externalMessageId = Uuid.random()

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

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.PUBLISHING_TO_KAFKA_FAILED
        }

        "Incoming message should not be processed if marking as downloaded fails" {
            val externalMessageId = Uuid.random()

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

            val errorMessage500 = EdiAdapterError.Api(500)
            ediAdapterClient.givenMarkAsDownloaded(Left(errorMessage500))

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.MARKING_MESSAGE_AS_DOWNLOADED_FAILED
        }

        "Incoming message should not be processed if sending apprec fails" {
            val externalMessageId = Uuid.random()

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
            ediAdapterClient.givenMarkAsDownloaded(Right(Unit))
            ediAdapterClient.givenPostApprecResponse(
                Left(
                    EdiAdapterError.Api(500)
                )
            )

            attachmentService.givenSaveAttachmentsEither(
                Unit.right()
            )

            publisher.givenPublishingResult(buildSuccessfulPublishingResult())

            val message = UnreadNotification(
                notificationId = Uuid.random(),
                relatedMessageId = externalMessageId,
                type = NotificationType.NEW_MESSAGE,
                notificationReceiverHerId = FAGSYSTEM_HER_ID
            )

            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.SENDING_APPREC_FAILED
        }
        "status notifications belong to outbound and are never deleted" {
            for (type in listOf(NotificationType.MESSAGE_SENT_STATE_UPDATED, NotificationType.MESSAGE_APPREC_INFO_UPDATED, NotificationType.MESSAGE_DELIVERY_STATE_UPDATED)) {
                notificationService.processNotification(notification(Uuid.random(), type))
            }
            ediAdapterClient.documentRequests shouldBe emptyList()
            ediAdapterClient.deleteRequests shouldBe emptyList()
            publisher.publishCount shouldBe 0
        }

        "refused notifications are persisted before deletion even without a message ID" {
            for (externalId in listOf(null, Uuid.random())) {
                val refused = notification(externalId, NotificationType.REFUSED_MESSAGE).copy(description = "Document refused")
                ediAdapterClient.beforeDelete = {
                    val saved = messageRepository.refusedMessages[refused.notificationId]!!
                    saved.externalMessageId shouldBe externalId
                    saved.receiverHerId shouldBe FAGSYSTEM_HER_ID
                    saved.description shouldBe "Document refused"
                }
                notificationService.processNotification(refused)
            }
            messageRepository.refusedMessages.size shouldBe 2
            ediAdapterClient.deleteRequests.size shouldBe 2
            ediAdapterClient.documentRequests shouldBe emptyList()
            ediAdapterClient.downloadedRequests shouldBe emptyList()
            ediAdapterClient.apprecRequests shouldBe emptyList()
            publisher.publishCount shouldBe 0
        }

        "missing related ID on a new message leaves the notification undeleted" {
            shouldThrow<NotificationProcessingException> {
                notificationService.processNotification(notification(null))
            }
            ediAdapterClient.deleteRequests shouldBe emptyList()
            ediAdapterClient.documentRequests shouldBe emptyList()
        }

        "successful handling is persisted before deleting the notification ID" {
            givenSuccessfulDocument()
            val message = notification(Uuid.random())
            ediAdapterClient.beforeDelete = {
                messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.SUCCESS
                ediAdapterClient.downloadedRequests.single().first shouldBe message.relatedMessageId
                val (id, request) = ediAdapterClient.apprecRequests.single()
                id shouldBe message.relatedMessageId
                request.appRecSenderHerId shouldBe FAGSYSTEM_HER_ID
                request.applicationName shouldBe "edi-adapter"
                request.applicationVersion shouldBe "1.0"
            }
            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)
        }

        "deletion failure leaves completed state and replay does not republish" {
            givenSuccessfulDocument()
            val message = notification(Uuid.random())
            ediAdapterClient.deleteError = EdiAdapterError.Api(503)
            notificationService.processNotification(message)
            messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.SUCCESS
            ediAdapterClient.deleteError = null
            notificationService.processNotification(message)
            publisher.publishCount shouldBe 1
            ediAdapterClient.documentRequests.size shouldBe 1
            ediAdapterClient.apprecRequests.size shouldBe 1
            ediAdapterClient.deleteRequests.size shouldBe 2
        }

        "retry after download failure does not republish" {
            givenSuccessfulDocument()
            val message = notification(Uuid.random())
            ediAdapterClient.givenMarkAsDownloaded(EdiAdapterError.Api(503).left())
            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()
            ediAdapterClient.givenMarkAsDownloaded(Unit.right())
            notificationService.processNotification(message)
            publisher.publishCount shouldBe 1
            ediAdapterClient.documentRequests.size shouldBe 1
            ediAdapterClient.downloadedRequests.size shouldBe 2
            ediAdapterClient.apprecRequests.size shouldBe 1
            messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.SUCCESS
        }

        "retry after AppRec failure only retries AppRec" {
            givenSuccessfulDocument()
            val message = notification(Uuid.random())
            ediAdapterClient.givenPostApprecResponse(EdiAdapterError.Api(503).left())
            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            ediAdapterClient.deleteRequests shouldBe emptyList()
            ediAdapterClient.givenPostApprecResponse(PostApprecResponse(Uuid.random()).right())
            notificationService.processNotification(message)
            publisher.publishCount shouldBe 1
            ediAdapterClient.downloadedRequests.size shouldBe 1
            ediAdapterClient.apprecRequests.size shouldBe 2
        }

        "invalid Base64 is recorded as a failure and leaves the notification undeleted" {
            ediAdapterClient.givenGetBusinessDocumentResponse(GetBusinessDocumentResponse("%%%", "application/xml", "base64").right())
            val message = notification(Uuid.random())
            shouldThrow<NotificationProcessingException> { notificationService.processNotification(message) }
            messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.DECODING_BUSINESS_DOCUMENT_FAILED
            ediAdapterClient.deleteRequests shouldBe emptyList()
            publisher.publishCount shouldBe 0
        }

        "persistence failure leaves a processed notification undeleted" {
            givenSuccessfulDocument()
            val failure = IllegalStateException("Database unavailable")
            val repository = object : MessageRepository by messageRepository {
                override suspend fun save(externalMessageId: Uuid, receivedAt: Instant, result: ProcessingResult) { throw failure }
            }
            val service = NotificationService(ediAdapterClient, publisher, attachmentService, messageConverter, FakeMetrics(), repository)
            shouldThrow<IllegalStateException> { service.processNotification(notification(Uuid.random())) } shouldBe failure
            ediAdapterClient.deleteRequests shouldBe emptyList()
        }

        "refusal persistence failure leaves the notification undeleted" {
            val repository = object : MessageRepository by messageRepository {
                override suspend fun saveRefusedMessage(message: RefusedMessage) { error("Database unavailable") }
            }
            val service = NotificationService(ediAdapterClient, publisher, attachmentService, messageConverter, FakeMetrics(), repository)
            shouldThrow<IllegalStateException> { service.processNotification(notification(null, NotificationType.REFUSED_MESSAGE)) }
            ediAdapterClient.deleteRequests shouldBe emptyList()
        }

        "terminal stream failure cancels the application scope" {
            ediAdapterClient.notifications = flowOf(EdiAdapterError.Api(401).left())
            val stopped = CompletableDeferred<Unit>()
            shouldThrow<NotificationProcessingException> {
                coroutineScope {
                    resourceScope {
                        val scope = resourceCoroutineScope(coroutineContext)
                        val started = CompletableDeferred<Unit>()
                        scope.launch {
                            try {
                                started.complete(Unit)
                                awaitCancellation()
                            } finally { stopped.complete(Unit) }
                        }
                        started.await()
                        notificationService.processNotifications(scope)
                        awaitCancellation()
                    }
                }
            }
            stopped.isCompleted shouldBe true
        }

        "stream handles only incoming types and preserves failed new message" {
            val refused = notification(null, NotificationType.REFUSED_MESSAGE)
            val newMessage = notification(Uuid.random())
            ediAdapterClient.notifications = flowOf(
                notification(Uuid.random(), NotificationType.MESSAGE_APPREC_INFO_UPDATED).right(),
                refused.right(),
                newMessage.right()
            )
            shouldThrow<NotificationProcessingException> {
                coroutineScope { notificationService.processNotifications(this).join() }
            }
            ediAdapterClient.notificationRequests shouldBe listOf(listOf(config().ediAdapter.receiverHerId))
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(refused.notificationId)
        }

        "cancelling consumption closes the notification flow" {
            val started = CompletableDeferred<Unit>()
            val closed = CompletableDeferred<Unit>()
            ediAdapterClient.notifications = flow {
                try {
                    started.complete(Unit)
                    awaitCancellation()
                } finally { closed.complete(Unit) }
            }
            val job = notificationService.processNotifications(this)
            started.await()
            job.cancelAndJoin()
            closed.isCompleted shouldBe true
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
    return NotificationServiceSpec::class.java.classLoader.getResource(path)!!.readText()
}

private fun MsgHeadMessageConverter.expectedPayload(xml: String): String =
    splitAttachments(xml)
        .shouldBeRight()
        .messageWithoutAttachmentsXml

private fun notification(externalId: Uuid?, type: NotificationType = NotificationType.NEW_MESSAGE): UnreadNotification =
    UnreadNotification(Uuid.random(), externalId, type, FAGSYSTEM_HER_ID)
