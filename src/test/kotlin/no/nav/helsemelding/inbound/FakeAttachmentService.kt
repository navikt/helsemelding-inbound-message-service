package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.service.AttachmentService

class FakeAttachmentService() : AttachmentService {
    private var splitMessage: SplitMessage? = null

    fun givenSplitMessage(splitMessage: SplitMessage) {
        this.splitMessage = splitMessage
    }

    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        return splitMessage!!
    }
}
