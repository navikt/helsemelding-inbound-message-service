package no.nav.helsemelding.inbound

import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.attachmentmodel.model.Attachment
import kotlin.uuid.Uuid

class FakeAttachmentClient : AttachmentClient {
    var savedMessageId: Uuid? = null
    var savedAttachments: List<Attachment>? = null
    var saveAttachmentsCallCount: Int = 0

    private var saveAttachmentsResult: Result<Unit> =
        Result.failure(IllegalStateException("Save attachments result is not set"))

    fun givenSaveAttachmentsResult(result: Result<Unit>) {
        saveAttachmentsResult = result
    }

    override suspend fun saveAttachments(
        messageId: Uuid,
        attachments: List<Attachment>
    ): Result<Unit> {
        saveAttachmentsCallCount += 1
        savedMessageId = messageId
        savedAttachments = attachments
        return saveAttachmentsResult
    }

    override suspend fun getAttachments(messageId: Uuid): Result<List<Attachment>> {
        return Result.success(emptyList())
    }

    override fun close() { }
}
