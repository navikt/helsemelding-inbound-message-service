package no.nav.helsemelding.inbound.service

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.model.ScanStatus
import no.nav.helsemelding.inbound.model.ValidationResult
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

fun interface ValidationService {
    suspend fun validate(splitMessage: SplitMessage, messageId: Uuid): ValidationResult
}

class MessageValidationService(
    private val clamAvClient: ClamAvClient
) : ValidationService {
    override suspend fun validate(
        splitMessage: SplitMessage,
        messageId: Uuid
    ): ValidationResult {
        val result = checkAttachmentsForViruses(splitMessage.attachments, messageId)
        if (result != ValidationResult.OK) return result

        log.debug { "Message $messageId validated successfully" }
        return ValidationResult.OK
    }

    private suspend fun checkAttachmentsForViruses(
        attachments: List<Attachment>,
        messageId: Uuid
    ): ValidationResult {
        if (attachments.isEmpty()) return ValidationResult.OK

        val results = clamAvClient.scanAttachments(attachments).fold(
            ifLeft = { error -> return scanningFailed(error, messageId) },
            ifRight = { it }
        )

        if (results.isEmpty()) {
            return ValidationResult.VIRUS_SCAN_FAILED
        }

        val isValid = results.all { it.result == ScanStatus.OK }

        if (!isValid) {
            log.warn { "ClamAV validation failed: scanning did not confirm safe attachments for message $messageId" }
            return ValidationResult.VIRUS_DETECTED
        }

        return ValidationResult.OK
    }

    private fun scanningFailed(error: Throwable, messageId: Uuid): ValidationResult {
        log.error(error) { "ClamAV validation failed for message $messageId" }
        return ValidationResult.VIRUS_SCAN_FAILED
    }
}
