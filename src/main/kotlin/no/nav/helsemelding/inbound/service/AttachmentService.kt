package no.nav.helsemelding.inbound.service

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.xml.MsgHeadSerializer
import no.nav.helsemelding.inbound.xml.extractAttachments
import no.nav.helsemelding.inbound.xml.removeAttachments
import no.nav.helsemelding.inbound.xml.toAttachment
import kotlin.uuid.Uuid
import no.nav.helsemelding.attachmentmodel.model.Attachment as AttachmentDto

private val log = KotlinLogging.logger {}

interface AttachmentService {
    fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage
    suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Boolean
}

class DomAttachmentService(
    val msgHeadSerializer: MsgHeadSerializer,
    val attachmentClient: AttachmentClient
) : AttachmentService {
    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        val msgHead = msgHeadSerializer.deserialize(msgHeadXml)

        val attachments = msgHead.extractAttachments()
            .map { it.toAttachment() }

        msgHead.removeAttachments()

        return SplitMessage(
            messageWithoutAttachmentXml = msgHeadSerializer.serialize(msgHead),
            attachments = attachments
        )
    }

    override suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Boolean {
        if (attachments.isEmpty()) return true

        val attachmentDtos = attachments.map {
            AttachmentDto(
                description = it.description,
                contentType = it.contentType,
                contentBase64 = it.contentBase64
            )
        }

        val response = attachmentClient.saveAttachments(messageId, attachmentDtos)

        if (response.isFailure) {
            val exception = response.exceptionOrNull()
            log.error(exception) { "Attachment test: Failed to save attachments for messageId: $messageId. Error: ${exception?.message}" }
            return false
        }

        log.debug { "Attachment test: Saved attachments for messageId: $messageId" }

        return true
    }
}
