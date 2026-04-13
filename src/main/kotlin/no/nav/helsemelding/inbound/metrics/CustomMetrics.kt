package no.nav.helsemelding.inbound.metrics

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

private val log = KotlinLogging.logger {}

interface Metrics {
    fun registerIncomingMessageReceived()
}

class CustomMetrics(val registry: MeterRegistry) : Metrics {

    override fun registerIncomingMessageReceived() {
        Counter.builder("helsemelding_incoming_messages_received")
            .description("Number of incoming messages received from NHN")
            .register(registry)
            .increment()
    }
}

class FakeMetrics() : Metrics {
    override fun registerIncomingMessageReceived() {
        log.info { "helsemelding_incoming_messages_received metric is registered" }
    }
}
