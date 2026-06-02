package no.nav.helsemelding.inbound.service

import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.xml.MsgHeadSerializer
import no.nav.helsemelding.inbound.xml.extractValidAttachments
import no.nav.helsemelding.inbound.xml.removeAttachments
import no.nav.helsemelding.inbound.xml.toAttachment

interface AttachmentService {
    fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage
}

class DomAttachmentService(
    val msgHeadSerializer: MsgHeadSerializer
) : AttachmentService {
    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        val msgHead = msgHeadSerializer.deserialize(msgHeadXml)

        val attachments = msgHead.extractValidAttachments()
            .map { it.toAttachment() }

        msgHead.removeAttachments()

        return SplitMessage(
            messageWithoutAttachment = msgHeadSerializer.serialize(msgHead),
            attachments = attachments
        )
    }
}
