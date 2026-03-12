package no.nav.helsemelding.inbound.service

import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.fx.coroutines.parMap
import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.GlobalOpenTelemetry
import kotlinx.coroutines.Dispatchers
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.GetMessagesRequest
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.publisher.MessagePublisher
import no.nav.helsemelding.inbound.util.withSpan
import java.util.Base64
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}
private val tracer = GlobalOpenTelemetry.getTracer("PollerService")

class PollerService(
    private val ediAdapterClient: EdiAdapterClient,
    private val messagePublisher: MessagePublisher
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
            log.info { "Processing message: ${message.id}" }
            when (message.isAppRec) {
                true -> {
                    log.info { "Processing apprec: ${message.id}" }
                    // TODO: Can be removed when outbound-message-service handles apprec
                    log.info { "Mark apprec as read: ${message.id}" }
                    markMessageAsRead(message.id!!, message.receiverHerId!!)
                }
                else -> {
                    log.info { "Processing incoming message: ${message.id}" }

                    val businessDocument = getBusinessDocument(message.id!!)
                    if (businessDocument != null) {
                        val isPublishingSuccessful = publishMessageToKafka(message.id!!, businessDocument)
                        if (isPublishingSuccessful) {
                            log.info { "Mark message as read: ${message.id}" }
                            markMessageAsRead(message.id!!, message.receiverHerId!!)
                        } else {
                            log.error { "Failed to publish message to Kafka: ${message.id}. Skipping the message!" }
                            false
                        }
                    } else {
                        log.error { "Failed to get business document for message: ${message.id}. Skipping the message!" }
                        false
                    }
                }
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
                null
            }
        }
    }

    private suspend fun publishMessageToKafka(messageId: Uuid, businessDocument: String): Boolean {
        val key = messageId.toString()
        val payload = Base64.getDecoder().decode(businessDocument)

        return messagePublisher.publish(key, payload)
            .map {
                log.info { "Successfully published message $key to Kafka." }
                true
            }
            .getOrElse { t ->
                log.error { "Failed to publish message $key: ${t.stackTraceToString()}" }
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
