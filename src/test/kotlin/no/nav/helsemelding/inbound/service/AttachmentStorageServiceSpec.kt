package no.nav.helsemelding.inbound.service

import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import no.nav.helsemelding.inbound.FakeAttachmentClient
import no.nav.helsemelding.inbound.model.Attachment
import kotlin.uuid.Uuid

class AttachmentStorageServiceSpec : StringSpec(
    {
        val attachmentClient = FakeAttachmentClient()
        val attachmentService = AttachmentStorageService(attachmentClient)

        "saveAttachments should return true if attachment are saved successfully" {
            val messageId = Uuid.random()
            val attachments = listOf(
                Attachment(
                    description = "Test attachment",
                    contentType = "text/plain",
                    contentBase64 = "VGhpcyBpcyBhIHRlc3QgYXR0YWNobWVudC4="
                )
            )

            attachmentClient.givenSaveAttachmentsResult(Result.success(Unit))

            val saveAttachmentResult = attachmentService.saveAttachments(messageId, attachments)

            saveAttachmentResult.shouldBeRight(Unit)
        }

        "saveAttachments should return true if there are no attachments to save" {
            val messageId = Uuid.random()
            val attachments = emptyList<Attachment>()

            val saveAttachmentResult = attachmentService.saveAttachments(messageId, attachments)

            saveAttachmentResult.shouldBeRight(Unit)
        }

        "saveAttachments should return left if saving attachments fails" {
            val messageId = Uuid.random()
            val attachments = listOf(
                Attachment(
                    description = "Test attachment",
                    contentType = "text/plain",
                    contentBase64 = "VGhpcyBpcyBhIHRlc3QgYXR0YWNobWVudC4="
                )
            )

            attachmentClient.givenSaveAttachmentsResult(Result.failure(Exception("Failed to save attachments")))

            val saveAttachmentResult = attachmentService.saveAttachments(messageId, attachments)

            saveAttachmentResult.shouldBeLeft()
        }
    }
)
