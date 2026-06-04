package no.nav.helsemelding.inbound.service

import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.xml.MsgHeadSerializer
import no.nav.helsemelding.inbound.xml.extractAttachments
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

        val attachments = msgHead.extractAttachments()
            .map { it.toAttachment() }

        msgHead.removeAttachments()

        return SplitMessage(
            messageWithoutAttachmentXml = msgHeadSerializer.serialize(msgHead),
            attachments = attachments
        )
    }
}
