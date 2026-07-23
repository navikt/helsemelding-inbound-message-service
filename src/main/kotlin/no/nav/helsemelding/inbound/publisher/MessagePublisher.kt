package no.nav.helsemelding.inbound.publisher

import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

const val ATTACHMENT_COUNT_HEADER = "attachments-count"

private val log = KotlinLogging.logger {}

interface MessagePublisher {
    suspend fun publish(
        key: String,
        payload: ByteArray,
        attachmentCount: Int
    ): Result<RecordMetadata>
}

class DialogMessagePublisher(
    private val kafkaPublisher: KafkaPublisher<String, ByteArray>
) : MessagePublisher {

    override suspend fun publish(
        key: String,
        payload: ByteArray,
        attachmentCount: Int
    ): Result<RecordMetadata> {
        val dialogMessageTopic = config().kafka.topics.dialogMessage

        val producerRecord = ProducerRecord(
            dialogMessageTopic,
            key,
            payload
        )

        producerRecord.headers().add(
            ATTACHMENT_COUNT_HEADER,
            attachmentCount.toString().toByteArray(Charsets.UTF_8)
        )

        return kafkaPublisher.publishScope {
            publishCatching(producerRecord)
        }
            .onSuccess {
                log.info { "Published incoming message $key to $dialogMessageTopic" }
            }
            .onFailure {
                    t ->
                log.error(t) { "Failed publishing incoming message: $key" }
            }
    }
}
