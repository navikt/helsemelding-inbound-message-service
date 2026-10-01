package no.nav.helsemelding.inbound.metrics

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

private val log = KotlinLogging.logger {}

interface Metrics {
    fun registerIncomingMessageReceived()
    fun registerIncomingMessageFailed(errorType: ErrorTypeTag)
    fun registerIncomingMessageProcessingDuration(durationNanos: Long)
}

class CustomMetrics(val registry: MeterRegistry) : Metrics {

    override fun registerIncomingMessageReceived() {
        Counter.builder("helsemelding_incoming_messages_received")
            .description("Number of incoming messages received from NHN")
            .register(registry)
            .increment()
    }

    override fun registerIncomingMessageFailed(errorType: ErrorTypeTag) {
        Counter.builder("helsemelding_incoming_messages_failed")
            .description("Number of incoming messages that failed to be processed")
            .tag("error_type", errorType.value)
            .register(registry)
            .increment()
    }

    override fun registerIncomingMessageProcessingDuration(durationNanos: Long) {
        Timer.builder("helsemelding_incoming_message_processing_duration")
            .description("Time spent processing an incoming message")
            .publishPercentileHistogram()
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }
}

class FakeMetrics() : Metrics {
    override fun registerIncomingMessageReceived() {
        log.info { "helsemelding_incoming_messages_received metric is registered" }
    }

    override fun registerIncomingMessageFailed(errorType: ErrorTypeTag) {
        log.info { "helsemelding_incoming_messages_failed metric is registered with error_type: ${errorType.value}" }
    }

    override fun registerIncomingMessageProcessingDuration(durationNanos: Long) {
        log.info { "helsemelding_incoming_message_processing_duration metric is registered with duration: $durationNanos ns" }
    }
}
