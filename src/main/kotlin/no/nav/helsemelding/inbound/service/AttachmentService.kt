package no.nav.helsemelding.inbound.service

import arrow.core.Either
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.util.toEither
import kotlin.uuid.Uuid
import no.nav.helsemelding.attachmentmodel.model.Attachment as ClientAttachment

private val log = KotlinLogging.logger {}

interface AttachmentService {
    suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Either<Throwable, Unit>
}

class AttachmentStorageService(
    private val attachmentClient: AttachmentClient
) : AttachmentService {
    override suspend fun saveAttachments(
        messageId: Uuid,
        attachments: List<Attachment>
    ): Either<Throwable, Unit> {
        val clientAttachments = attachments.map { attachment ->
            ClientAttachment(
                description = attachment.description,
                contentType = attachment.contentType,
                contentBase64 = attachment.contentBase64
            )
        }

        return attachmentClient
            .saveAttachments(messageId, clientAttachments)
            .toEither()
            .onLeft { error ->
                log.error(error) {
                    "Failed to save attachments for messageId: $messageId. Error: ${error.message}"
                }
            }
    }
}
