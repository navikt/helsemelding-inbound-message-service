package no.nav.helsemelding.inbound.publisher

import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

private val log = KotlinLogging.logger {}

interface MessagePublisher {
    suspend fun publish(key: String, payload: ByteArray): Result<RecordMetadata>
}

class DialogMessagePublisher(
    private val kafkaPublisher: KafkaPublisher<String?, ByteArray>
) : MessagePublisher {

    override suspend fun publish(key: String, payload: ByteArray): Result<RecordMetadata> {
        val dialogMessageTopic = config().kafka.topics.dialogMessage

        return kafkaPublisher.publishScope {
            publishCatching(
                ProducerRecord(
                    dialogMessageTopic,
                    key,
                    payload
                )
            )
        }
            .onSuccess {
                log.info { "Published incoming message $key to $dialogMessageTopic" }
            }
            .onFailure {
                    t ->
                log.error { "Failed publishing incoming message $key: ${t.stackTraceToString()}" }
            }
    }
}
