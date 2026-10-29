package no.nav.helsemelding.inbound.service

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.model.ScanStatus
import no.nav.helsemelding.inbound.model.ProcessingResult
import no.nav.helsemelding.inbound.repository.MessageRepository
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

fun interface ValidationService {
    suspend fun validate(
        splitMessage: SplitMessage,
        messageId: Uuid,
        contentHash: String?
    ): ProcessingResult
}

class MessageValidationService(
    private val clamAvClient: ClamAvClient,
    private val messageRepository: MessageRepository
) : ValidationService {
    override suspend fun validate(
        splitMessage: SplitMessage,
        messageId: Uuid,
        contentHash: String?
    ): ProcessingResult {
        val duplicateResult = checkDuplicates(contentHash, messageId)
        if (duplicateResult != ProcessingResult.SUCCESS) return duplicateResult

        val scanResult = checkAttachmentsForViruses(splitMessage.attachments, messageId)
        if (scanResult != ProcessingResult.SUCCESS) return scanResult

        log.debug { "Message $messageId validated successfully" }
        return ProcessingResult.SUCCESS
    }

    private suspend fun checkDuplicates(contentHash: String?, messageId: Uuid): ProcessingResult {
        if (contentHash != null && messageRepository.findByContentHash(contentHash) != null) {
            log.warn { "Message $messageId rejected because its content matches a previously processed message" }
            return ProcessingResult.DUPLICATE_MESSAGE_DETECTED
        }
        return ProcessingResult.SUCCESS
    }

    private suspend fun checkAttachmentsForViruses(
        attachments: List<Attachment>,
        messageId: Uuid
    ): ProcessingResult {
        if (attachments.isEmpty()) return ProcessingResult.SUCCESS

        val results = clamAvClient.scanAttachments(attachments).fold(
            ifLeft = { error -> return scanningFailed(error, messageId) },
            ifRight = { it }
        )

        if (results.isEmpty()) {
            return ProcessingResult.VIRUS_SCAN_FAILED
        }

        val isValid = results.all { it.result == ScanStatus.OK }

        if (!isValid) {
            log.warn { "ClamAV validation failed: scanning did not confirm safe attachments for message $messageId" }
            return ProcessingResult.VIRUS_DETECTED
        }

        return ProcessingResult.SUCCESS
    }

    private fun scanningFailed(error: Throwable, messageId: Uuid): ProcessingResult {
        log.error(error) { "ClamAV validation failed for message $messageId" }
        return ProcessingResult.VIRUS_SCAN_FAILED
    }
}
