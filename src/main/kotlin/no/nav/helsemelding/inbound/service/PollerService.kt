package no.nav.helsemelding.inbound.service

import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.fx.coroutines.parMap
import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.GlobalOpenTelemetry
import kotlinx.coroutines.Dispatchers
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.model.AppRecStatus
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.GetMessagesRequest
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.ediadapter.model.Metadata
import no.nav.helsemelding.ediadapter.model.PostAppRecRequest
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.metrics.ErrorTypeTag
import no.nav.helsemelding.inbound.metrics.Metrics
import no.nav.helsemelding.inbound.publisher.MessagePublisher
import no.nav.helsemelding.inbound.util.registerDuration
import no.nav.helsemelding.inbound.util.withSpan
import java.util.Base64
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}
private val tracer = GlobalOpenTelemetry.getTracer("PollerService")

class PollerService(
    private val ediAdapterClient: EdiAdapterClient,
    private val messagePublisher: MessagePublisher,
    private val attachmentService: AttachmentService,
    private val metrics: Metrics
) {
    private val pollerConfig = config().poller

    suspend fun pollMessages() {
        log.info { "=== Poll cycle start ===" }
        val cycleStart = System.currentTimeMillis()

        fetchMessages(pollerConfig.fetchLimit, pollerConfig.herId)
            .withLogging()
            .chunked(pollerConfig.batchSize)
            .forEach { processBatch(it) }

        log.info { "=== Poll cycle end: ${System.currentTimeMillis() - cycleStart}ms ===" }
    }

    private suspend fun fetchMessages(messagesToFetch: Int, herId: Int): List<Message> {
        val getMessagesRequest = GetMessagesRequest(
            receiverHerIds = listOf(herId),
            includeMetadata = true,
            messagesToFetch = messagesToFetch
        )

        return when (val messages = ediAdapterClient.getMessages(getMessagesRequest)) {
            is Right<List<Message>> ->
                messages.value
                    .filter { it.id != null }
                    .filter { it.receiverHerId != null }

            is Left<ErrorMessage> -> {
                log.error { "Failed to get messages for herId: $herId. Error: ${messages.value}" }
                emptyList()
            }
        }
    }

    private suspend fun processBatch(batch: List<Message>) {
        val summary = batch.batchSummary()
        log.info { "Processing ($summary)" }

        logBatchDuration(summary) {
            batch.parMap(Dispatchers.IO) { processMessage(it) }
        }
    }

    internal suspend fun processMessage(message: Message): Boolean {
        return tracer.withSpan("Process incoming message") {
            val messageId = requireNotNull(message.id)
            val receiverHerId = requireNotNull(message.receiverHerId)

            log.info { "Processing message: $messageId" }
            when (message.isAppRec) {
                true -> processAppRec(messageId, receiverHerId)
                else -> registerDuration(metrics::registerIncomingMessageProcessingDuration) {
                    processIncomingMessage(messageId, receiverHerId)
                }
            }
        }
    }

    private suspend fun processAppRec(messageId: Uuid, receiverHerId: Int): Boolean {
        // TODO: Can be removed when outbound-message-service handles apprec
        log.info { "Processing apprec: $messageId" }
        metrics.registerIncomingMessageReceived(true)

        return markMessageAsRead(messageId, receiverHerId)
    }

    private suspend fun processIncomingMessage(messageId: Uuid, receiverHerId: Int): Boolean {
        log.info { "Processing incoming message: $messageId" }
        metrics.registerIncomingMessageReceived()

        val businessDocument = getBusinessDocument(messageId) ?: return false

        val payload = String(Base64.getDecoder().decode(businessDocument))
        val splitMessage = attachmentService.splitMsgHeadAndAttachments(payload)

        val isPublishingSuccessful = publishMessageToKafka(messageId, splitMessage.messageWithoutAttachmentXml)
        if (!isPublishingSuccessful) return false

        val isMarkedAsRead = markMessageAsRead(messageId, receiverHerId)
        if (!isMarkedAsRead) return false

        // TODO: Temporary solution. Application receipt should be sent as a result of receiving feedback from fagsystem.
        return sendAppRec(messageId, receiverHerId)
    }

    private suspend fun sendAppRec(messageId: Uuid, receiverHerId: Int): Boolean {
        val appRecRequest = PostAppRecRequest(
            appRecStatus = AppRecStatus.OK
        )

        val response = ediAdapterClient.postApprec(messageId, receiverHerId, appRecRequest)
        return when (response) {
            is Right<Metadata> -> {
                log.info { "Apprec sent successfully for message: $messageId" }
                true
            }
            is Left<ErrorMessage> -> {
                log.error { "Failed to send apprec for message: $messageId. Error: ${response.value}" }
                metrics.registerIncomingMessageFailed(ErrorTypeTag.SENDING_APPREC_FAILED)
                false
            }
        }
    }

    private suspend fun markMessageAsRead(messageId: Uuid, herId: Int): Boolean {
        return when (val either = ediAdapterClient.markMessageAsRead(messageId, herId)) {
            is Right<Boolean> -> {
                log.info { "Successfully marked message: $messageId as read." }
                either.value
            }

            is Left<ErrorMessage> -> {
                log.error { "Failed to mark message: $messageId as read. Error: ${either.value}" }
                metrics.registerIncomingMessageFailed(ErrorTypeTag.MARKING_MESSAGE_AS_READ_FAILED)
                false
            }
        }
    }

    private suspend fun getBusinessDocument(messageId: Uuid): String? {
        return when (val response = ediAdapterClient.getBusinessDocument(messageId)) {
            is Right<GetBusinessDocumentResponse> -> {
                log.info { "Retrieved business document for message: $messageId" }
                response.value.businessDocument
            }
            is Left<ErrorMessage> -> {
                log.error { "Failed to retrieve business document for message: $messageId: ${response.value.error} Stack trace: ${response.value.stackTrace}" }
                metrics.registerIncomingMessageFailed(ErrorTypeTag.RETRIEVING_BUSINESS_DOCUMENT_FAILED)
                null
            }
        }
    }

    private suspend fun publishMessageToKafka(messageId: Uuid, payload: String): Boolean {
        val key = messageId.toString()

        return messagePublisher.publish(key, payload.toByteArray())
            .map {
                log.info { "Successfully published message $key to Kafka." }
                true
            }
            .getOrElse { t ->
                log.error(t) { "Failed to publish message to Kafka: $key" }
                metrics.registerIncomingMessageFailed(ErrorTypeTag.PUBLISHING_TO_KAFKA_FAILED)
                false
            }
    }

    private fun List<Message>.withLogging(): List<Message> = also {
        log.info { "Fetched messages size=$size" }
    }

    private fun List<Message>.batchSummary(): String =
        when (size) {
            0 -> "batchSize=0"
            1 -> "batchSize=1 id=${first().id}"
            else -> "batchSize=$size first=${first().id} last=${last().id}"
        }

    private inline fun <T> logBatchDuration(summary: String, block: () -> T): T {
        val start = System.currentTimeMillis()
        return try {
            block()
        } finally {
            log.info { "Batch completed ($summary took ${System.currentTimeMillis() - start}ms)" }
        }
    }
}
