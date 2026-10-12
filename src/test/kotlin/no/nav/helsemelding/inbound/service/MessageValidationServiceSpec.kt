package no.nav.helsemelding.inbound.service

import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.messageconverter.MsgHeadMessageConverter

class MessageValidationServiceSpec : StringSpec(
    {
        "Validation should accept messages with and without attachments" {
            val validationService = MessageValidationService()
            val messageConverter = MsgHeadMessageConverter()

            listOf("message_without_attachments.xml", "message_with_attachments.xml").forEach { resource ->
                val xml = readFileToString(resource)
                val splitMessage = messageConverter.splitAttachments(xml).shouldBeRight()

                validationService.validate(splitMessage) shouldBe true
            }
        }
    }
)
