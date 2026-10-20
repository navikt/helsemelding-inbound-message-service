package no.nav.helsemelding.inbound

import arrow.continuations.SuspendApp
import arrow.continuations.ktor.server
import arrow.core.raise.Raise
import arrow.core.raise.result
import arrow.fx.coroutines.resourceScope
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.Application
import io.ktor.server.netty.Netty
import io.ktor.utils.io.CancellationException
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.awaitCancellation
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.model.v3.MshConfiguration
import no.nav.helsemelding.ediadapter.model.v3.ReceiveNotificationChannel.API
import no.nav.helsemelding.ediadapter.model.v3.SetMshConfigurationsRequest
import no.nav.helsemelding.inbound.metrics.CustomMetrics
import no.nav.helsemelding.inbound.persistence.repository.ExposedMessageRepository
import no.nav.helsemelding.inbound.plugin.configureMetrics
import no.nav.helsemelding.inbound.plugin.configureRoutes
import no.nav.helsemelding.inbound.publisher.DialogMessagePublisher
import no.nav.helsemelding.inbound.service.AttachmentStorageService
import no.nav.helsemelding.inbound.service.MessageValidationService
import no.nav.helsemelding.inbound.service.NotificationService
import no.nav.helsemelding.inbound.util.coroutineScope
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter

private val log = KotlinLogging.logger {}

fun main() = SuspendApp {
    result {
        resourceScope {
            val deps = dependencies()

            setMshConfiguration(
                deps.ediAdapterClient,
                config().ediAdapter.receiverHerId
            )

            val metrics = CustomMetrics(deps.meterRegistry)

            server(
                Netty,
                port = config().server.port.value,
                preWait = config().server.preWait,
                module = inboundModule(deps.meterRegistry)
            )

            val dialogMessagePublisher = DialogMessagePublisher(deps.kafkaPublisher)
            val attachmentService = AttachmentStorageService(deps.attachmentClient)
            val messageConverter = MsgHeadMessageConverter()

            val notificationService = NotificationService(
                deps.ediAdapterClient,
                dialogMessagePublisher,
                attachmentService,
                messageConverter,
                metrics,
                ExposedMessageRepository(deps.database),
                MessageValidationService(deps.clamAvClient)
            )

            val scope = coroutineScope(coroutineContext)
            notificationService.processNotifications(scope)

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

private suspend fun Raise<Throwable>.setMshConfiguration(ediAdapterClient: EdiAdapterClient, senderHerId: Int) {
    ediAdapterClient.setMshConfigurations(
        SetMshConfigurationsRequest(
            listOf(
                MshConfiguration(
                    herId = senderHerId,
                    receiveNotificationChannel = API
                )
            )
        )
    )
        .mapLeft { error -> MshConfigurationException("Unable to configure MSH: $error") }
        .bind()

    log.info { "MSH configured for herId: $senderHerId (notificationChannel: $API)" }
}

private fun logError(t: Throwable) = log.error { "Shutdown inbound-message-service due to: ${t.stackTraceToString()}" }

private class MshConfigurationException(message: String) : RuntimeException(message)
