package no.nav.helsemelding.inbound.model

data class SplitMessage(
    val messageWithoutAttachment: String,
    val attachments: List<Attachment>
)
