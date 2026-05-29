package no.nav.helsemelding.inbound.service

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import java.nio.file.Files
import java.nio.file.Paths

private val MESSAGE_WITH_ATTACHMENTS_PATH = "src/test/resources/message_with_attachments.xml"
private val MESSAGE_WITHOUT_ATTACHMENTS_PATH = "src/test/resources/message_without_attachments.xml"

class AttachmentServiceSpec : StringSpec({

    "should split fellesformat message and attachments" {
        val attachmentService = AttachmentService()

        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITH_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndVedlegg(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult.vedlegg.size shouldBe 3
    }

    "should process fellesformat message when it does not contain attachments" {
        val attachmentService = AttachmentService()

        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITHOUT_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndVedlegg(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult.vedlegg.size shouldBe 0
    }
})
