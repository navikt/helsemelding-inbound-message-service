package no.nav.helsemelding.inbound

import arrow.core.Either
import arrow.core.left
import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.service.AttachmentService
import kotlin.uuid.Uuid

class FakeAttachmentService : AttachmentService {
    var savedMessageId: Uuid? = null
        private set
    var savedAttachments: List<Attachment>? = null
        private set
    var saveAttachmentsCallCount: Int = 0
        private set

    private var saveAttachmentsEither: Either<Throwable, Unit> =
        IllegalStateException("Save attachments result is not set").left()

    fun givenSaveAttachmentsEither(either: Either<Throwable, Unit>) {
        this.saveAttachmentsEither = either
    }

    override suspend fun saveAttachments(messageId: Uuid, attachments: List<Attachment>): Either<Throwable, Unit> {
        saveAttachmentsCallCount += 1
        savedMessageId = messageId
        savedAttachments = attachments

        return saveAttachmentsEither
    }
}
