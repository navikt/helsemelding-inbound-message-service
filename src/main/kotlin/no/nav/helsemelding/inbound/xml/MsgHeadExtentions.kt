package no.nav.helsemelding.inbound.xml

import no.nav.helse.base64container.Base64Container
import no.nav.helse.msgHead.XMLDocument
import no.nav.helse.msgHead.XMLMsgHead
import no.nav.helsemelding.inbound.model.Attachment
import java.util.Base64

const val ATTACHMENT_TYPE = "A"

private val acceptedMimeTypes = listOf(
    "application/pdf",
    "image/tiff",
    "image/png",
    "image/jpeg",
    "image/pjpeg",
    "image/jpg",
    "image/pjpg"
)

fun XMLMsgHead.extractAttachments(): List<XMLDocument> {
    val attachments = mutableListOf<XMLDocument>()

    attachments.addAll(extractAllAttachmentsFromMsgHead())

    document.forEach { document ->
        document.refDoc.content.any.forEach {
            if (it is XMLMsgHead) {
                attachments.addAll(it.extractAttachments())
            }
        }
    }

    return attachments
}

fun XMLMsgHead.extractAllAttachmentsFromMsgHead(): List<XMLDocument> =
    this.document.filter { it.isAttachment() }

fun XMLMsgHead.removeAttachments() {
    document.removeAll { xmlDocument ->
        xmlDocument.isAttachment()
    }

    document.forEach { xmlDocument ->
        xmlDocument.refDoc.content.any.forEach { content ->
            if (content is XMLMsgHead) {
                content.removeAttachments()
            }
        }
    }
}

fun XMLDocument.isAttachment(): Boolean =
    refDoc.msgType.v == ATTACHMENT_TYPE && acceptedMimeTypes.contains(refDoc.mimeType)

fun XMLDocument.toAttachment(): Attachment =
    Attachment(
        description = refDoc.description ?: "",
        contentType = refDoc.mimeType,
        contentBase64 = Base64.getEncoder().encodeToString(toBase64Container().value)
    )

fun XMLDocument.toBase64Container(): Base64Container =
    refDoc.content.any[0] as Base64Container
