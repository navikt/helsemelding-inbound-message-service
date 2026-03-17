package no.nav.helsemelding.inbound.util

import no.nav.helsemelding.inbound.publisher.MessagePublisher
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

class LocalMessagePublisher : MessagePublisher {
    override suspend fun publish(key: String, payload: ByteArray): Result<RecordMetadata> {
        return Result.success(
            RecordMetadata(
                TopicPartition("test", 0),
                0L,
                0,
                System.currentTimeMillis(),
                0,
                0
            )
        )
    }
}
