package no.nav.helsemelding.inbound.xml

import no.nav.helse.base64container.Base64Container
import no.nav.helse.msgHead.XMLDocument
import no.nav.helse.msgHead.XMLMsgHead
import no.nav.helsemelding.inbound.model.Attachment
import java.util.HexFormat

private val PDF_MAGIC_NUMBER: ByteArray = HexFormat.of().parseHex("255044462D")

fun XMLMsgHead.extractValidAttachments(): List<XMLDocument> =
    this.extractAllAttachments().filter {
        it.isAttachment() && it.pdfContentMatchesMimeType()
    }

fun XMLMsgHead.extractAllAttachments(): List<XMLDocument> {
    val attachments = mutableListOf<XMLDocument>()

    attachments.addAll(extractAllAttachmentsFromMsgHead())

    document.forEach { document ->
        document.refDoc.content.any.forEach {
            if (it is XMLMsgHead) {
                attachments.addAll(it.extractAllAttachments())
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
    refDoc.msgType.v == "A" &&
        listOf(
            "application/pdf",
            "image/tiff",
            "image/png",
            "image/jpeg",
            "image/pjpeg",
            "image/jpg",
            "image/pjpg"
        ).contains(refDoc.mimeType)

fun XMLDocument.toAttachment(): Attachment =
    Attachment(
        description = refDoc.description ?: "",
        contentType = refDoc.mimeType,
        contentBase64 = String(toBase64Container().value)
    )

fun XMLDocument.pdfContentMatchesMimeType(): Boolean =
    refDoc.mimeType != "application/pdf" || toBase64Container().value.binaryContentIsPdf()

fun XMLDocument.toBase64Container(): Base64Container =
    refDoc.content.any[0] as Base64Container

fun ByteArray.binaryContentIsPdf(): Boolean =
    copyOf(PDF_MAGIC_NUMBER.size) contentEquals PDF_MAGIC_NUMBER
