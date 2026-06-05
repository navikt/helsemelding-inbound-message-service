package no.nav.helsemelding.inbound

import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.attachmentmodel.model.Attachment
import kotlin.uuid.Uuid

class FakeAttachmentClient : AttachmentClient {
    var saveAttachmentsResult: Result<Unit>? = null

    fun givenSaveAttachmentsResult(result: Result<Unit>) {
        saveAttachmentsResult = result
    }

    override suspend fun saveAttachments(
        messageId: Uuid,
        attachments: List<Attachment>
    ): Result<Unit> {
        return saveAttachmentsResult!!
    }

    override suspend fun getAttachments(messageId: Uuid): Result<List<Attachment>> {
        return Result.success(emptyList())
    }

    override fun close() { }
}
