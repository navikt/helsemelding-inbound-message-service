package no.nav.helsemelding.inbound.plugin

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.attachmentclient.HttpAttachmentClient
import no.nav.helsemelding.attachmentmodel.model.Attachment
import java.lang.Thread.sleep
import kotlin.uuid.Uuid

fun Application.configureRoutes(
    registry: PrometheusMeterRegistry
) {
    routing { internalRoutes(registry) }
}

fun Route.internalRoutes(registry: PrometheusMeterRegistry) {
    get("/prometheus") {
        call.respond(registry.scrape())
    }
    route("/internal") {
        get("/health/liveness") {
            call.respondText("I'm alive! :)")
        }
        get("/health/readiness") {
            call.respondText("I'm ready! :)")
        }
    }

    get("/test-attachments") {
        try {
            val messageId = Uuid.random()

            val testAttachments = listOf(
                Attachment(
                    description = "attachment 1",
                    contentType = "text/plain",
                    contentBase64 = "VGhpcyBpcyBhIHRlc3QgYXR0YWNobWVudC4=$messageId"
                ),
                Attachment(
                    description = "PDF file",
                    contentType = "application/pdf",
                    contentBase64 = "JVBERi0xLjQKJcfsj6IKNSAwIG9ia="
                )
            )

            val attachmentClient = HttpAttachmentClient()

            val saveResult = attachmentClient.saveAttachments(
                messageId = messageId,
                attachments = testAttachments
            )

            sleep(2000)

            val readResult = attachmentClient.getAttachments(messageId)

            call.respond(HttpStatusCode.OK, readResult.getOrNull().orEmpty().size)
        } catch (e: Exception) {
            call.respond(HttpStatusCode.InternalServerError, "Error testing attachments: ${e.message}")
        }
    }
}
