package no.nav.helsemelding.inbound

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.client.HttpEdiAdapterClient
import no.nav.helsemelding.ediadapter.client.scopedAuthHttpClient
import no.nav.helsemelding.inbound.config.EdiAdapter

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val ediAdapterClient: EdiAdapterClient
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed prometheus registry" } }
    }

internal suspend fun ResourceScope.ediAdapterClient(ediAdapter: EdiAdapter): EdiAdapterClient =
    install({ HttpEdiAdapterClient(scopedAuthHttpClient(ediAdapter.scope.value)) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed edi adapter client" } }
    }

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val ediAdapterClient = async { ediAdapterClient(config.ediAdapter) }

    Dependencies(
        metricsRegistry.await(),
        ediAdapterClient.await()
    )
}
