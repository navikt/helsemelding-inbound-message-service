package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.model.SplitMessage
import no.nav.helsemelding.inbound.service.AttachmentService
import kotlin.uuid.Uuid

class FakeAttachmentService() : AttachmentService {
    private var splitMessageResult: Result<SplitMessage> =
        Result.failure(IllegalStateException("Split message result is not set"))

    private var saveAttachmentsResult: Result<Unit> =
        Result.failure(IllegalStateException("Save attachments result is not set"))

    fun givenSplitMessageResult(actionResult: Result<SplitMessage>) {
        this.splitMessageResult = actionResult
    }

    fun givenSaveAttachmentResult(actionResult: Result<Unit>) {
        this.saveAttachmentsResult = actionResult
    }

    override fun splitMsgHeadAndAttachments(msgHeadXml: String): Result<SplitMessage> {
        return splitMessageResult
    }

    override suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Result<Unit> {
        return saveAttachmentsResult
    }
}
