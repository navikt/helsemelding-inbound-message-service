package no.nav.helsemelding.inbound.client

import arrow.core.Either
import arrow.core.flatten
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.request.accept
import io.ktor.client.request.forms.formData
import io.ktor.client.request.forms.submitFormWithBinaryData
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.PartData
import kotlinx.serialization.json.Json
import no.nav.helsemelding.inbound.config
import no.nav.helsemelding.inbound.model.ScanResult
import no.nav.helsemelding.messageconverter.model.Attachment
import java.io.IOException
import java.util.Base64

private val log = KotlinLogging.logger {}

class ClamAvClient(
    private val httpClient: HttpClient
) {
    private val endpointUrl = config().clamAv.url
    private val json = Json { ignoreUnknownKeys = true }

    suspend fun scanAttachments(attachments: List<Attachment>): Either<Throwable, List<ScanResult>> = Either.catch {
        when {
            attachments.isEmpty() -> {
                log.debug { "Skipping ClamAV scan: no attachments" }
                emptyList<ScanResult>().right()
            }
            else -> readScanResults(sendScanRequest(attachments))
        }
    }
        .flatten()
        .onLeft { error -> log.error(error) { "ClamAV scan request failed" } }

    private suspend fun sendScanRequest(attachments: List<Attachment>): HttpResponse {
        log.debug { "Scanning ${attachments.size} attachments with ClamAV" }
        return httpClient.submitFormWithBinaryData(
            url = "${endpointUrl.trimEnd('/')}/api/v2/scan",
            formData = attachments.toFormData()
        ) { accept(ContentType.Application.Json) }
            .also { response -> log.debug { "ClamAV responded with HTTP ${response.status.value}" } }
    }

    private fun List<Attachment>.toFormData(): List<PartData> = formData {
        forEachIndexed { index, attachment ->
            append(
                key = "file$index",
                value = Base64.getDecoder().decode(attachment.contentBase64),
                headers = Headers.build {
                    append(HttpHeaders.ContentType, attachment.contentType)
                    append(HttpHeaders.ContentDisposition, "filename=\"file$index\"")
                }
            )
        }
    }

    private suspend fun readScanResults(response: HttpResponse): Either<Throwable, List<ScanResult>> = either {
        ensure(response.status == HttpStatusCode.OK) {
            IOException("ClamAV scan failed with HTTP ${response.status.value}")
        }
        json.decodeFromString<List<ScanResult>>(response.bodyAsText())
    }
        .onRight { results -> log.debug { "ClamAV returned ${results.size} scan results" } }
}
