package no.nav.helsemelding.inbound.service

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.model.ScanStatus
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

fun interface ValidationService {
    suspend fun isValid(splitMessage: SplitMessage, messageId: Uuid): Boolean
}

class MessageValidationService(
    private val clamAvClient: ClamAvClient
) : ValidationService {
    override suspend fun isValid(
        splitMessage: SplitMessage,
        messageId: Uuid
    ): Boolean {
        if (!isAttachmentSafe(splitMessage.attachments, messageId)) return false

        log.debug { "Message $messageId validated successfully" }
        return true
    }

    private suspend fun isAttachmentSafe(
        attachments: List<Attachment>,
        messageId: Uuid
    ): Boolean {
        if (attachments.isEmpty()) return true

        val results = try {
            clamAvClient.scanAttachments(attachments)
        } catch (error: Exception) {
            if (error is CancellationException) throw error
            return scanningFailed(error, messageId)
        }

        val valid = results.isNotEmpty() && results.all {
            it.result == ScanStatus.OK
        }

        if (!valid) {
            log.warn { "ClamAV validation failed: scanning did not confirm safe attachments for message $messageId" }
        }
        return valid
    }

    private fun scanningFailed(error: Exception, messageId: Uuid): Boolean {
        log.error(error) { "ClamAV validation failed for message $messageId" }
        return false
    }
}
