package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.service.AttachmentService
import kotlin.uuid.Uuid

class FakeAttachmentService() : AttachmentService {
    private var splitMessage: SplitMessage? = null

    fun givenSplitMessage(splitMessage: SplitMessage) {
        this.splitMessage = splitMessage
    }

    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage {
        return splitMessage!!
    }

    override suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Boolean {
        return true
    }
}
