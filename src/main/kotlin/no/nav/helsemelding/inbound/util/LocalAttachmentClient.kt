package no.nav.helsemelding.inbound.util

import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.attachmentmodel.model.Attachment
import kotlin.uuid.Uuid

private val log = KotlinLogging.logger {}

class LocalAttachmentClient : AttachmentClient {

    private val storedAttachments = mutableMapOf<Uuid, List<Attachment>>()

    override suspend fun saveAttachments(
        messageId: Uuid,
        attachments: List<Attachment>
    ): Result<Unit> {
        log.info {
            "LocalAttachmentClient: Saving ${attachments.size} attachment(s) for message: $messageId"
        }

        storedAttachments[messageId] = attachments

        return Result.success(Unit)
    }

    override suspend fun getAttachments(
        messageId: Uuid
    ): Result<List<Attachment>> {
        return Result.success(
            storedAttachments[messageId] ?: emptyList()
        )
    }

    override fun close() {}
}
