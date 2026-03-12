package no.nav.helsemelding.inbound

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.publisher.MessagePublisher
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

private val log = KotlinLogging.logger {}

class FakeMessagePublisher : MessagePublisher {
    var publishedKey: String? = null
    var publishedPayload: ByteArray? = null

    override suspend fun publish(key: String, payload: ByteArray): Result<RecordMetadata> {
        publishedKey = key
        publishedPayload = payload

        val record = RecordMetadata(
            TopicPartition("test", 0),
            0L,
            0,
            System.currentTimeMillis(),
            key.length,
            payload.size
        )

        log.info { "Published message: $key" }

        return Result.success(record)
    }
}
