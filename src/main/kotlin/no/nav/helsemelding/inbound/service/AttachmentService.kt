package no.nav.helsemelding.inbound.service

import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.xml.MsgHeadSerializer
import no.nav.helsemelding.inbound.xml.extractValidVedlegg
import no.nav.helsemelding.inbound.xml.removeVedlegg
import no.nav.helsemelding.inbound.xml.toVedlegg

interface AttachmentService {
    fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage
}

class DomAttachmentService(
    val msgHeadSerializer: MsgHeadSerializer
) : AttachmentService {
    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        val msgHead = msgHeadSerializer.deserialize(msgHeadXml)

        val attachments = msgHead.extractValidVedlegg()
            .map { it.toVedlegg() }

        msgHead.removeVedlegg()

        return SplitMessage(
            messageWithoutAttachment = msgHeadSerializer.serialize(msgHead),
            attachments = attachments
        )
    }
}
