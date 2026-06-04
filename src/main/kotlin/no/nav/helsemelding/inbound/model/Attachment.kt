package no.nav.helsemelding.inbound.model

data class Attachment(
    val description: String,
    val contentType: String,
    val contentBase64: String
)
