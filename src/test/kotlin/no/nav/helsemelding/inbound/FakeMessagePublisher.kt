package no.nav.helsemelding.inbound

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.publisher.MessagePublisher
import org.apache.kafka.clients.producer.RecordMetadata

private val log = KotlinLogging.logger {}

class FakeMessagePublisher : MessagePublisher {
    var publishedKey: String? = null
    var publishedPayload: ByteArray? = null
    private var publishingResult: Result<RecordMetadata>? = null

    fun givenPublishingResult(result: Result<RecordMetadata>) {
        publishingResult = result
    }

    override suspend fun publish(key: String, payload: ByteArray): Result<RecordMetadata> {
        publishedKey = key
        publishedPayload = payload

        return publishingResult!!
    }
}
