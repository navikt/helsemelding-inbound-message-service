package no.nav.helsemelding.inbound.service

import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.xml.MsgHeadMarshaller
import no.nav.helsemelding.inbound.xml.extractValidVedlegg
import no.nav.helsemelding.inbound.xml.removeVedlegg
import no.nav.helsemelding.inbound.xml.toVedlegg

class AttachmentService {
    private val msgHeadMarshaller = MsgHeadMarshaller()

    fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        val msgHead = msgHeadMarshaller.unmarshalXML(msgHeadXml)

        val attachments = msgHead.extractValidVedlegg()
            .map { it.toVedlegg() }

        msgHead.removeVedlegg()

        return SplitMessage(
            messageWithoutAttachment = msgHeadMarshaller.marshalMsgHead(msgHead),
            attachments = attachments
        )
    }
}
