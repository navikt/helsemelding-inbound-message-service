package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.service.AttachmentService
import kotlin.uuid.Uuid

class FakeAttachmentService() : AttachmentService {
    private var splitMessage: SplitMessage? = null
    private var isAttachmentsSaved: Boolean = true

    fun givenSplitMessage(splitMessage: SplitMessage?) {
        this.splitMessage = splitMessage
    }

    fun givenIsAttachmentsSaved(isSaved: Boolean) {
        this.isAttachmentsSaved = isSaved
    }

    override fun splitMsgHeadAndAttachments(msgHeadXml: String): SplitMessage? {
        return splitMessage
    }

    override suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Boolean {
        return isAttachmentsSaved
    }
}
