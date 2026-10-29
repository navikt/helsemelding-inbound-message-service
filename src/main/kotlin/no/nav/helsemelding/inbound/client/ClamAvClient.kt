package no.nav.helsemelding.inbound.client

import arrow.core.Either
import arrow.core.left
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
import kotlinx.coroutines.CancellationException
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

    suspend fun scanAttachments(attachments: List<Attachment>): Either<Throwable, List<ScanResult>> {
        if (attachments.isEmpty()) {
            log.debug { "Skipping ClamAV scan: no attachments" }
            return emptyList<ScanResult>().right()
        }

        log.debug { "Scanning ${attachments.size} attachments with ClamAV" }

        return try {
            val formData = createAttachmentFormData(attachments)
            val response = sendScanRequest(formData)
            readScanResults(response).right()
        } catch (error: CancellationException) {
            throw error
        } catch (error: Exception) {
            log.error(error) { "ClamAV scan request failed" }
            error.left()
        }
    }

    private fun createAttachmentFormData(attachments: List<Attachment>): List<PartData> = formData {
        attachments.forEachIndexed { index, attachment ->
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

    private suspend fun sendScanRequest(formData: List<PartData>): HttpResponse =
        httpClient.submitFormWithBinaryData(
            url = "${endpointUrl.trimEnd('/')}/api/v2/scan",
            formData = formData
        ) {
            accept(ContentType.Application.Json)
        }

    private suspend fun readScanResults(response: HttpResponse): List<ScanResult> {
        log.debug { "ClamAV responded with HTTP ${response.status.value}" }

        if (response.status != HttpStatusCode.OK) {
            throw IOException("ClamAV scan failed with HTTP ${response.status.value}")
        }

        val results = json.decodeFromString<List<ScanResult>>(response.bodyAsText())
        log.debug { "ClamAV returned ${results.size} scan results" }
        return results
    }
}
