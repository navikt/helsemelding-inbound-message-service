package no.nav.helsemelding.inbound.model

data class SplitMessage(
    val messageWithoutAttachmentXml: String,
    val attachments: List<Attachment>
)
