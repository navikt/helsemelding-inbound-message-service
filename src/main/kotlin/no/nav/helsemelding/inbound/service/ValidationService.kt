package no.nav.helsemelding.inbound.service

import arrow.core.getOrElse
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.inbound.client.ClamAvClient
import no.nav.helsemelding.inbound.model.ScanStatus
import no.nav.helsemelding.inbound.persistence.model.ValidationResult
import no.nav.helsemelding.jsonschema.core.model.IncomingDialogMessageType
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter
import no.nav.helsemelding.messageconverter.json.IncomingDialogMessageSerializer
import no.nav.helsemelding.messageconverter.model.Attachment
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

fun interface ValidationService {
    suspend fun validate(splitMessage: SplitMessage, messageId: Uuid): ValidationResult
}

class MessageValidationService(
    private val clamAvClient: ClamAvClient,
    private val messageConverter: MsgHeadMessageConverter = MsgHeadMessageConverter()
) : ValidationService {
    private val incomingDialogMessageSerializer = IncomingDialogMessageSerializer()

    override suspend fun validate(
        splitMessage: SplitMessage,
        messageId: Uuid
    ): ValidationResult {
        val messageTextResult = checkMessageText(splitMessage.messageWithoutAttachmentsXml, messageId)
        if (messageTextResult != ValidationResult.OK) return messageTextResult

        val result = checkAttachmentsForViruses(splitMessage.attachments, messageId)
        if (result != ValidationResult.OK) return result

        log.debug { "Message $messageId validated successfully" }
        return ValidationResult.OK
    }

    private fun checkMessageText(xml: String, messageId: Uuid): ValidationResult {
        val json = messageConverter.incomingDialogMessageXmlToJson(xml).getOrElse {
            log.debug { "Skipping message-text validation because message $messageId could not be mapped" }
            return ValidationResult.OK
        }
        val message = incomingDialogMessageSerializer.deserialize(json).getOrElse {
            log.debug { "Skipping message-text validation because message $messageId could not be deserialized" }
            return ValidationResult.OK
        }

        if (message.type in messageTypesRequiringText && message.message.isNullOrEmpty()) {
            log.warn { "Message text missing for incoming message $messageId" }
            return ValidationResult.MISSING_MESSAGE_TEXT
        }

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

    private companion object {
        val messageTypesRequiringText = setOf(
            IncomingDialogMessageType.PATIENT_REQUEST_RESPONSE,
            IncomingDialogMessageType.SICK_LEAVE_FOLLOW_UP_INQUIRY,
            IncomingDialogMessageType.PATIENT_INQUIRY
        )
    }
}
