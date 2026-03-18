package no.nav.helsemelding.inbound

import arrow.continuations.SuspendApp
import arrow.continuations.ktor.server
import arrow.core.raise.result
import arrow.fx.coroutines.resourceScope
import arrow.resilience.Schedule
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.Application
import io.ktor.server.netty.Netty
import io.ktor.utils.io.CancellationException
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.awaitCancellation
import no.nav.helsemelding.inbound.plugin.configureMetrics
import no.nav.helsemelding.inbound.plugin.configureRoutes
import no.nav.helsemelding.inbound.publisher.DialogMessagePublisher
import no.nav.helsemelding.inbound.service.PollerService

private val log = KotlinLogging.logger {}

fun main() = SuspendApp {
    result {
        resourceScope {
            val deps = dependencies()

            server(
                Netty,
                port = config().server.port.value,
                preWait = config().server.preWait,
                module = inboundModule(deps.meterRegistry)
            )

            val dialogMessagePublisher = DialogMessagePublisher(deps.kafkaPublisher)
            val poller = PollerService(deps.ediAdapterClient, dialogMessagePublisher)

            scheduleProcessMessages(poller)

            awaitCancellation()
        }
    }
        .onFailure { error -> if (error !is CancellationException) logError(error) }
}

internal fun inboundModule(
    meterRegistry: PrometheusMeterRegistry
): Application.() -> Unit {
    return {
        configureMetrics(meterRegistry)
        configureRoutes(meterRegistry)
    }
}

private suspend fun scheduleProcessMessages(processor: PollerService): Long =
    Schedule
        .spaced<Unit>(config().poller.scheduleInterval)
        .repeat { processor.pollMessages() }

private fun logError(t: Throwable) = log.error { "Shutdown inbound-message-service due to: ${t.stackTraceToString()}" }
