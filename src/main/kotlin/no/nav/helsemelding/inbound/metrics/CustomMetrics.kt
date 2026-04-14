package no.nav.helsemelding.inbound.metrics

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

private val log = KotlinLogging.logger {}

interface Metrics {
    fun registerIncomingMessageReceived(isApprec: Boolean = false)
}

class CustomMetrics(val registry: MeterRegistry) : Metrics {

    override fun registerIncomingMessageReceived(isApprec: Boolean) {
        Counter.builder("helsemelding_incoming_messages_received")
            .description("Number of incoming messages received from NHN")
            .tag("isApprec", isApprec.toString())
            .register(registry)
            .increment()
    }
}

class FakeMetrics() : Metrics {
    override fun registerIncomingMessageReceived(isApprec: Boolean) {
        log.info { "helsemelding_incoming_messages_received metric is registered" }
    }
}
