package no.nav.helsemelding.inbound.service

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import no.nav.helsemelding.inbound.FakeAttachmentClient
import no.nav.helsemelding.inbound.model.Attachment
import no.nav.helsemelding.inbound.xml.JaxbMsgHeadSerializer
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.uuid.Uuid

private val MESSAGE_WITH_ATTACHMENTS_PATH = "src/test/resources/message_with_attachments.xml"
private val MESSAGE_WITHOUT_ATTACHMENTS_PATH = "src/test/resources/message_without_attachments.xml"

class DomAttachmentServiceSpec : StringSpec({

    val msgHeadSerializer = JaxbMsgHeadSerializer()
    val attachmentClient = FakeAttachmentClient()
    val attachmentService = DomAttachmentService(msgHeadSerializer, attachmentClient)

    "splitMsgHeadAndAttachments should split XML message and attachments" {
        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITH_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndAttachments(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult!!.attachments.size shouldBe 3
        splitResult.messageWithoutAttachmentXml shouldContain "<MsgInfo>"
        splitResult.messageWithoutAttachmentXml shouldNotContain "Base64Container"
    }

    "splitMsgHeadAndAttachments should process XML message when it does not contain attachments" {
        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITHOUT_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndAttachments(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult!!.attachments.size shouldBe 0
        splitResult.messageWithoutAttachmentXml shouldContain "<MsgInfo>"
    }

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

        val saveResult = attachmentService.saveAttachments(messageId, attachments)

        saveResult shouldBe true
    }

    "saveAttachments should return true if there are no attachments to save" {
        val messageId = Uuid.random()
        val attachments = emptyList<Attachment>()

        val saveResult = attachmentService.saveAttachments(messageId, attachments)

        saveResult shouldBe true
    }

    "saveAttachments should return false if saving attachments fails" {
        val messageId = Uuid.random()
        val attachments = listOf(
            Attachment(
                description = "Test attachment",
                contentType = "text/plain",
                contentBase64 = "VGhpcyBpcyBhIHRlc3QgYXR0YWNobWVudC4="
            )
        )

        attachmentClient.givenSaveAttachmentsResult(Result.failure(Exception("Failed to save attachments")))

        val saveResult = attachmentService.saveAttachments(messageId, attachments)

        saveResult shouldBe false
    }
})
