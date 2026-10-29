package no.nav.helsemelding.inbound.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.getOrElse
import arrow.core.raise.either
import arrow.core.raise.ensure
import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.GlobalOpenTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.model.v3.AppRecStatus
import no.nav.helsemelding.ediadapter.model.v3.DeleteNotificationsRequest
import no.nav.helsemelding.ediadapter.model.v3.MarkAsDownloadedRequest
import no.nav.helsemelding.ediadapter.model.v3.NotificationType.NEW_MESSAGE
import no.nav.helsemelding.ediadapter.model.v3.NotificationType.REFUSED_MESSAGE
import no.nav.helsemelding.ediadapter.model.v3.PostAppRecRequest
import no.nav.helsemelding.ediadapter.model.v3.UnreadNotification
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.metrics.ErrorTypeTag
import no.nav.helsemelding.inbound.metrics.Metrics
import no.nav.helsemelding.inbound.model.MessageProcessingOutcome
import no.nav.helsemelding.inbound.model.ProcessingResult
import no.nav.helsemelding.inbound.model.ProcessingResult.CALCULATING_CONTENT_HASH_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.DECODING_BUSINESS_DOCUMENT_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.DUPLICATE_MESSAGE_DETECTED
import no.nav.helsemelding.inbound.model.ProcessingResult.EXTRACTING_MESSAGE_ID_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.IGNORED_APPREC
import no.nav.helsemelding.inbound.model.ProcessingResult.MARKING_MESSAGE_AS_DOWNLOADED_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.PUBLISHING_TO_KAFKA_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.RETRIEVING_BUSINESS_DOCUMENT_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.SAVING_ATTACHMENTS_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.SENDING_APPREC_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.SPLITTING_MESSAGE_FAILED
import no.nav.helsemelding.inbound.model.ProcessingResult.SUCCESS
import no.nav.helsemelding.inbound.publisher.MessagePublisher
import no.nav.helsemelding.inbound.repository.MessageRepository
import no.nav.helsemelding.inbound.util.registerDuration
import no.nav.helsemelding.inbound.util.toEither
import no.nav.helsemelding.inbound.util.withSpan
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter
import no.nav.helsemelding.messageconverter.XmlMessageClassifier.isAppRec
import no.nav.helsemelding.messageconverter.model.SplitMessage
import java.util.Base64
import kotlin.time.Clock
import kotlin.uuid.Uuid
import no.nav.helsemelding.attachmentmodel.model.Attachment as ClientAttachment
import no.nav.helsemelding.messageconverter.model.Attachment as ConverterAttachment

private val log = KotlinLogging.logger {}
private val tracer = GlobalOpenTelemetry.getTracer("NotificationService")

class NotificationService(
    private val ediAdapterClient: EdiAdapterClient,
    private val messagePublisher: MessagePublisher,
    private val attachmentClient: AttachmentClient,
    private val messageConverter: MsgHeadMessageConverter,
    private val metrics: Metrics,
    private val messageRepository: MessageRepository,
    private val validationService: ValidationService
) {
    fun processNotifications(scope: CoroutineScope): Job {
        val herId = config().ediAdapter.receiverHerId
        log.info { "Starting unread notification stream for herId: $herId" }
        return ediAdapterClient.streamUnreadNotifications(herId)
            .onEach { result ->
                result
                    .onLeft { throw NotificationProcessingException("Notification stream failed for herId $herId: $it") }
                    .onRight { processNotification(it) }
            }
            .flowOn(Dispatchers.IO)
            .launchIn(scope)
    }

    internal suspend fun processNotification(notification: UnreadNotification) {
        when (notification.type) {
            NEW_MESSAGE -> processNewMessageNotification(notification)
            REFUSED_MESSAGE -> processRefusedMessageNotification(notification)
            else -> return // Status notifications belong to outbound-message-service.
        }
        deleteNotification(notification.notificationId)
    }

    private suspend fun processNewMessageNotification(notification: UnreadNotification) {
        val externalMessageId = notification.relatedMessageId ?: run {
            log.error { "New message notification ${notification.notificationId} has no related message ID" }
            metrics.registerIncomingMessageFailed(ErrorTypeTag.MISSING_EXTERNAL_MESSAGE_ID)
            return
        }
        if (messageRepository.findByExternalMessageId(externalMessageId) != null) return

        tracer.withSpan("Process incoming message") {
            registerDuration(metrics::registerIncomingMessageProcessingDuration) {
                processAndRecordMessage(
                    externalMessageId,
                    notification.notificationReceiverHerId
                )
            }
        }
    }

    private suspend fun processAndRecordMessage(externalMessageId: Uuid, receiverHerId: Int) {
        val receivedAt = Clock.System.now()

        val messageProcessingOutcome = processNewMessage(externalMessageId, receiverHerId)
        val result = messageProcessingOutcome.result
        if (result == IGNORED_APPREC) return

        messageRepository.save(
            externalMessageId,
            receivedAt,
            messageProcessingOutcome
        )

        metrics.registerIncomingMessageReceived()
        if (result != SUCCESS) {
            metrics.registerIncomingMessageFailed(ErrorTypeTag.valueOf(result.name))
            log.error { "Failed processing message $externalMessageId: $result" }
        }
    }

    private fun processRefusedMessageNotification(notification: UnreadNotification) {
        log.warn { "Incoming message refused: notificationId=${notification.notificationId} externalMessageId=${notification.relatedMessageId}" }
    }

    private suspend fun processNewMessage(
        externalMessageId: Uuid,
        receiverHerId: Int
    ): MessageProcessingOutcome = either<ProcessingResult, MessageProcessingOutcome> {
        val xml = fetchBusinessDocument(externalMessageId).bind()
        ensure(!isAppRec(xml).getOrElse { false }) { IGNORED_APPREC }

        val contentHash = calculateContentHash(xml).bind()
        val messageId = extractMessageId(externalMessageId, xml).bind()
        val splitMessage = splitAttachments(externalMessageId, xml).bind()

        val validationFailure = validateMessage(splitMessage, messageId, contentHash)
        if (validationFailure != null) return@either validationFailure

        saveAttachments(messageId, splitMessage.attachments).bind()
        publishMessage(messageId, splitMessage).bind()
        markMessageAsDownloaded(externalMessageId, receiverHerId).bind()

        sendAppRec(externalMessageId, receiverHerId).bind()

        log.info { "Successfully processed incoming message $externalMessageId" }
        MessageProcessingOutcome(SUCCESS, contentHash)
    }
        .getOrElse { MessageProcessingOutcome(it, null) }

    private suspend fun validateMessage(
        splitMessage: SplitMessage,
        messageId: Uuid,
        contentHash: String?
    ): MessageProcessingOutcome? {
        val validationResult = validationService.validate(splitMessage, messageId, contentHash)
        return validationResult.processingResult?.let { failure ->
            log.error { "Validation failed for message $messageId: $validationResult" }
            MessageProcessingOutcome(
                failure,
                contentHash.takeIf { failure == DUPLICATE_MESSAGE_DETECTED }
            )
        }
    }

    private suspend fun fetchBusinessDocument(externalMessageId: Uuid): Either<ProcessingResult, String> =
        ediAdapterClient.getBusinessDocument(externalMessageId)
            .onLeft { log.error { "Failed retrieving document for $externalMessageId: $it" } }
            .mapLeft { RETRIEVING_BUSINESS_DOCUMENT_FAILED }
            .flatMap { decodeBusinessDocument(externalMessageId, it.businessDocument) }

    private fun decodeBusinessDocument(externalMessageId: Uuid, base64: String): Either<ProcessingResult, String> =
        Either.catch {
            Base64.getDecoder().decode(base64).toString(Charsets.UTF_8)
        }
            .onLeft { failure -> log.error(failure) { "Invalid Base64 document for $externalMessageId" } }
            .mapLeft { DECODING_BUSINESS_DOCUMENT_FAILED }

    private fun extractMessageId(externalMessageId: Uuid, xml: String): Either<ProcessingResult, Uuid> =
        messageConverter.extractMessageId(xml)
            .onLeft { log.error { "Failed extracting XML message ID for $externalMessageId: $it" } }
            .mapLeft { EXTRACTING_MESSAGE_ID_FAILED }

    private fun splitAttachments(externalMessageId: Uuid, xml: String): Either<ProcessingResult, SplitMessage> =
        messageConverter.splitAttachments(xml)
            .onLeft { log.error { "Failed splitting document for $externalMessageId: $it" } }
            .mapLeft { SPLITTING_MESSAGE_FAILED }

    private suspend fun saveAttachments(
        messageId: Uuid,
        attachments: List<ConverterAttachment>
    ): Either<ProcessingResult, Unit> = either {
        if (attachments.isNotEmpty()) {
            val clientAttachments = attachments.map {
                ClientAttachment(
                    description = it.description,
                    contentType = it.contentType,
                    contentBase64 = it.contentBase64
                )
            }
            attachmentClient.saveAttachments(messageId, clientAttachments)
                .toEither()
                .onLeft { failure -> log.error(failure) { "Failed saving attachments for $messageId" } }
                .mapLeft { SAVING_ATTACHMENTS_FAILED }
                .bind()
        }
    }

    private suspend fun publishMessage(messageId: Uuid, message: SplitMessage): Either<ProcessingResult, Unit> =
        either {
            messagePublisher.publish(
                messageId.toString(),
                message.messageWithoutAttachmentsXml.toByteArray(Charsets.UTF_8),
                message.attachments.size
            )
                .getOrElse { failure ->
                    log.error(failure) { "Failed publishing message $messageId to Kafka" }
                    raise(PUBLISHING_TO_KAFKA_FAILED)
                }
        }

    private suspend fun markMessageAsDownloaded(
        externalMessageId: Uuid,
        receiverHerId: Int
    ): Either<ProcessingResult, Unit> =
        ediAdapterClient.markMessageAsDownloaded(externalMessageId, MarkAsDownloadedRequest(receiverHerId))
            .onLeft { log.error { "Failed marking message $externalMessageId as downloaded: $it" } }
            .mapLeft { MARKING_MESSAGE_AS_DOWNLOADED_FAILED }

    private suspend fun sendAppRec(externalMessageId: Uuid, receiverHerId: Int): Either<ProcessingResult, Unit> {
        // TODO: Send AppRec after feedback from fagsystem instead of immediately after publication.
        return ediAdapterClient.postApprec(
            externalMessageId,
            PostAppRecRequest(
                appRecSenderHerId = receiverHerId,
                appRecStatus = AppRecStatus.OK
            )
        )
            .onLeft { log.error { "Failed sending AppRec for $externalMessageId: $it" } }
            .mapLeft { SENDING_APPREC_FAILED }
            .map { }
    }

    private suspend fun deleteNotification(notificationId: Uuid) {
        ediAdapterClient.deleteNotifications(DeleteNotificationsRequest(listOf(notificationId)))
            .onLeft { failure ->
                log.error { "Failed deleting notification $notificationId: $failure" }
                metrics.registerIncomingMessageFailed(ErrorTypeTag.DELETING_NOTIFICATION_FAILED)
            }
            .onRight { log.info { "Successfully deleted notification $notificationId" } }
    }

    private fun calculateContentHash(xml: String): Either<ProcessingResult, String> = either {
        val messageId = messageConverter.extractMessageId(xml)
            .onLeft { log.error { "Failed extracting XML message ID for content hash calculation: $it" } }
            .mapLeft { EXTRACTING_MESSAGE_ID_FAILED }
            .bind()

        messageConverter.calculateContentHash(xml)
            .onLeft { log.error { "Failed calculating content hash for message $messageId" } }
            .mapLeft { CALCULATING_CONTENT_HASH_FAILED }
            .bind()
    }
}

internal class NotificationProcessingException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)
