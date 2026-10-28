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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
import no.nav.helsemelding.inbound.FakeValidationService
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.metrics.CustomMetrics
import no.nav.helsemelding.inbound.metrics.FakeMetrics
import no.nav.helsemelding.inbound.metrics.Metrics
import no.nav.helsemelding.inbound.persistence.model.ProcessedMessage
import no.nav.helsemelding.inbound.persistence.model.ProcessingResult
import no.nav.helsemelding.inbound.persistence.model.ValidationResult
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
        lateinit var validationService: FakeValidationService

        fun createNotificationService(
            metrics: Metrics = FakeMetrics(),
            repository: MessageRepository = messageRepository
        ): NotificationService = NotificationService(
            ediAdapterClient,
            publisher,
            attachmentService,
            messageConverter,
            metrics,
            repository,
            validationService
        )

        beforeEach {
            ediAdapterClient = FakeEdiAdapterClient()
            publisher = FakeMessagePublisher()
            attachmentService = FakeAttachmentService()
            messageConverter = MsgHeadMessageConverter()
            messageRepository = FakeMessageRepository()
            validationService = FakeValidationService()
            notificationService = createNotificationService()
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

        "NEW_MESSAGE with a standalone AppRec is deleted without processing or recording a failure" {
            val registry = SimpleMeterRegistry()
            val service = createNotificationService(metrics = CustomMetrics(registry))
            val documents = listOf(
                """
                <AppRec xmlns="http://www.kith.no/xmlstds/apprec/2004-11-21">
                    <MsgType V="APPREC" DN="Applikasjonskvittering"/>
                </AppRec>
                """
                    .trimIndent(),
                """
                <?xml version="1.0"?>
                <!-- AppRec -->
                <a:AppRec xmlns:a="http://www.kith.no/xmlstds/apprec/2004-11-21">
                    <a:MsgType V="APPREC" DN="Applikasjonskvittering"/>
                </a:AppRec>
                """
                    .trimIndent()
            )
            for (xml in documents) {
                ediAdapterClient.givenGetBusinessDocumentResponse(
                    GetBusinessDocumentResponse(
                        Base64.getEncoder().encodeToString(xml.toByteArray()),
                        "application/xml",
                        "base64"
                    )
                        .right()
                )
                val message = notification(Uuid.random())

                service.processNotification(message)

                messageRepository.findByExternalMessageId(message.relatedMessageId!!) shouldBe null
                ediAdapterClient.deleteRequests.last().notificationIds shouldBe listOf(message.notificationId)
            }
            ediAdapterClient.downloadedRequests shouldBe emptyList()
            ediAdapterClient.apprecRequests shouldBe emptyList()
            publisher.publishCount shouldBe 0
            attachmentService.saveAttachmentsCallCount shouldBe 0
            validationService.validatedMessage shouldBe null
            registry.find("helsemelding_incoming_messages_received").counter() shouldBe null
            registry.find("helsemelding_incoming_messages_failed").counter() shouldBe null
        }

        "MsgHead containing an AppRec is not mistaken for a standalone AppRec" {
            val apprec = """
                <AppRec xmlns="http://www.kith.no/xmlstds/apprec/2004-11-21">
                    <MsgType V="APPREC" DN="Applikasjonskvittering"/>
                </AppRec>
            """
                .trimIndent()
            val xml = readFileToString("message/incomingDialogMessage.xml")
                .replace("</Content>", "$apprec</Content>")
            ediAdapterClient.givenGetBusinessDocumentResponse(
                GetBusinessDocumentResponse(
                    Base64.getEncoder().encodeToString(xml.toByteArray()),
                    "application/xml",
                    "base64"
                )
                    .right()
            )
            publisher.givenPublishingResult(buildSuccessfulPublishingResult())
            val message = notification(Uuid.random())

            notificationService.processNotification(message)

            messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.SUCCESS
            publisher.publishCount shouldBe 1
            ediAdapterClient.apprecRequests.single().first shouldBe message.relatedMessageId
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
            validationService.validatedMessage shouldBe messageConverter.splitAttachments(xml).shouldBeRight()
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

            validationService.validatedMessage shouldBe messageConverter.splitAttachments(xml).shouldBeRight()
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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.EXTRACTING_MESSAGE_ID_FAILED
        }

        for ((validationResult, expectedResult, errorTag) in listOf(
            Triple(ValidationResult.VIRUS_DETECTED, ProcessingResult.VIRUS_DETECTED, "validating_virus_detected"),
            Triple(ValidationResult.VIRUS_SCAN_FAILED, ProcessingResult.VIRUS_SCAN_FAILED, "validating_virus_scan_failed")
        )) {
            "$validationResult is recorded once and prevents attachment storage and publication" {
                val xml = readFileToString("message_with_attachments.xml")
                ediAdapterClient.givenGetBusinessDocumentResponse(
                    GetBusinessDocumentResponse(
                        Base64.getEncoder().encodeToString(xml.toByteArray()),
                        "application/xml",
                        "base64"
                    ).right()
                )
                validationService.givenValidationResult(validationResult)
                val registry = SimpleMeterRegistry()
                val service = createNotificationService(metrics = CustomMetrics(registry))
                val message = notification(Uuid.random())
                ediAdapterClient.beforeDelete = {
                    messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe expectedResult
                    registry.get("helsemelding_incoming_messages_failed")
                        .tag("error_type", errorTag).counter().count() shouldBe 1.0
                }

                service.processNotification(message)

                validationService.validatedMessage shouldBe messageConverter.splitAttachments(xml).shouldBeRight()
                attachmentService.saveAttachmentsCallCount shouldBe 0
                publisher.publishCount shouldBe 0
                ediAdapterClient.downloadedRequests shouldBe emptyList()
                ediAdapterClient.apprecRequests shouldBe emptyList()
                ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)
            }
        }

        "Incoming message with missing text should not be published" {
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
            validationService.givenValidationResult(ValidationResult.MISSING_MESSAGE_TEXT)

            val message = notification(externalMessageId)

            notificationService.processNotification(message)

            attachmentService.saveAttachmentsCallCount shouldBe 0
            publisher.publishedPayload shouldBe null

            val result = messageRepository.findByExternalMessageId(externalMessageId)!!.result
            result shouldBe ProcessingResult.MISSING_MESSAGE_TEXT
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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

            notificationService.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)

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

        "refused notifications are deleted without accessing the database" {
            val repository = object : MessageRepository {
                override suspend fun save(externalMessageId: Uuid, receivedAt: Instant, result: ProcessingResult) {
                    error("Refused notifications must not be persisted")
                }
                override suspend fun findByExternalMessageId(externalMessageId: Uuid): ProcessedMessage? =
                    error("Refused notifications must not access the database")
            }
            val service = createNotificationService(repository = repository)
            val refusedNotifications = listOf(null, Uuid.random()).map {
                notification(it, NotificationType.REFUSED_MESSAGE)
            }
            for (refused in refusedNotifications) {
                service.processNotification(refused)
            }
            ediAdapterClient.deleteRequests.map { it.notificationIds.single() } shouldBe
                refusedNotifications.map { it.notificationId }
            ediAdapterClient.documentRequests shouldBe emptyList()
            ediAdapterClient.downloadedRequests shouldBe emptyList()
            ediAdapterClient.apprecRequests shouldBe emptyList()
            attachmentService.saveAttachmentsCallCount shouldBe 0
            publisher.publishCount shouldBe 0
            validationService.validatedMessage shouldBe null
        }

        "missing related ID is counted and the notification is deleted" {
            val registry = SimpleMeterRegistry()
            val service = createNotificationService(metrics = CustomMetrics(registry))
            val message = notification(null)
            service.processNotification(message)
            registry.get("helsemelding_incoming_messages_failed").tag("error_type", "missing_external_message_id").counter().count() shouldBe 1.0
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)
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

        "registered failures are never reprocessed when their notification is redelivered" {
            val failures = ProcessingResult.entries.filter {
                it != ProcessingResult.SUCCESS && it != ProcessingResult.IGNORED_APPREC
            }
            for (result in failures) {
                val message = notification(Uuid.random())
                messageRepository.save(message.relatedMessageId!!, kotlin.time.Clock.System.now(), result)
                notificationService.processNotification(message)
                messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe result
            }
            ediAdapterClient.documentRequests shouldBe emptyList()
            ediAdapterClient.downloadedRequests shouldBe emptyList()
            ediAdapterClient.apprecRequests shouldBe emptyList()
            publisher.publishCount shouldBe 0
            ediAdapterClient.deleteRequests.size shouldBe failures.size
        }

        "a failed message is persisted and counted before deletion" {
            val registry = SimpleMeterRegistry()
            val service = createNotificationService(metrics = CustomMetrics(registry))
            val message = notification(Uuid.random())
            ediAdapterClient.beforeDelete = {
                messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.RETRIEVING_BUSINESS_DOCUMENT_FAILED
                registry.get("helsemelding_incoming_messages_failed").tag("error_type", "retrieving_business_document_failed").counter().count() shouldBe 1.0
            }
            service.processNotification(message)
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)
        }

        "invalid Base64 is recorded as a failure and the notification is deleted" {
            ediAdapterClient.givenGetBusinessDocumentResponse(GetBusinessDocumentResponse("%%%", "application/xml", "base64").right())
            val message = notification(Uuid.random())
            notificationService.processNotification(message)
            messageRepository.findByExternalMessageId(message.relatedMessageId!!)!!.result shouldBe ProcessingResult.DECODING_BUSINESS_DOCUMENT_FAILED
            ediAdapterClient.deleteRequests.single().notificationIds shouldBe listOf(message.notificationId)
            publisher.publishCount shouldBe 0
        }

        "persistence failure leaves a processed notification undeleted" {
            givenSuccessfulDocument()
            val failure = IllegalStateException("Database unavailable")
            val repository = object : MessageRepository by messageRepository {
                override suspend fun save(externalMessageId: Uuid, receivedAt: Instant, result: ProcessingResult) { throw failure }
            }
            val service = createNotificationService(repository = repository)
            shouldThrow<IllegalStateException> { service.processNotification(notification(Uuid.random())) } shouldBe failure
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

        "stream records failures and continues with the next incoming message" {
            val refused = notification(null, NotificationType.REFUSED_MESSAGE)
            val failedMessage = notification(Uuid.random())
            val successfulMessage = notification(Uuid.random())
            ediAdapterClient.notifications = flow {
                emit(notification(Uuid.random(), NotificationType.MESSAGE_APPREC_INFO_UPDATED).right())
                emit(refused.right())
                emit(failedMessage.right())
                givenSuccessfulDocument()
                emit(successfulMessage.right())
            }
            notificationService.processNotifications(this).join()
            ediAdapterClient.notificationRequests shouldBe listOf(listOf(config().ediAdapter.receiverHerId))
            ediAdapterClient.deleteRequests.map { it.notificationIds.single() } shouldBe
                listOf(refused.notificationId, failedMessage.notificationId, successfulMessage.notificationId)
            messageRepository.findByExternalMessageId(failedMessage.relatedMessageId!!)!!.result shouldBe ProcessingResult.RETRIEVING_BUSINESS_DOCUMENT_FAILED
            messageRepository.findByExternalMessageId(successfulMessage.relatedMessageId!!)!!.result shouldBe ProcessingResult.SUCCESS
            publisher.publishCount shouldBe 1
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
