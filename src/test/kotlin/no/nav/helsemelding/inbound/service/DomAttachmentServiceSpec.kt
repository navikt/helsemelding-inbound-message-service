package no.nav.helsemelding.inbound.service

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import no.nav.helsemelding.inbound.FakeAttachmentClient
import no.nav.helsemelding.inbound.xml.JaxbMsgHeadSerializer
import java.nio.file.Files
import java.nio.file.Paths

private val MESSAGE_WITH_ATTACHMENTS_PATH = "src/test/resources/message_with_attachments.xml"
private val MESSAGE_WITHOUT_ATTACHMENTS_PATH = "src/test/resources/message_without_attachments.xml"

class DomAttachmentServiceSpec : StringSpec({

    val msgHeadSerializer = JaxbMsgHeadSerializer()
    val attachmentClient = FakeAttachmentClient()
    val attachmentService = DomAttachmentService(msgHeadSerializer, attachmentClient)

    "should split XML message and attachments" {
        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITH_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndAttachments(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult.attachments.size shouldBe 3
        splitResult.messageWithoutAttachmentXml shouldContain "<MsgInfo>"
        splitResult.messageWithoutAttachmentXml shouldNotContain "Base64Container"
    }

    "should process XML message when it does not contain attachments" {
        val messageWithAttachments = String(Files.readAllBytes(Paths.get(MESSAGE_WITHOUT_ATTACHMENTS_PATH)))

        val splitResult = attachmentService.splitMsgHeadAndAttachments(messageWithAttachments)

        splitResult shouldNotBe null
        splitResult.attachments.size shouldBe 0
        splitResult.messageWithoutAttachmentXml shouldContain "<MsgInfo>"
    }
})
