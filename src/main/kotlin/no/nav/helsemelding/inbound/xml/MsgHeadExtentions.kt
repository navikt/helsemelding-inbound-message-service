package no.nav.helsemelding.inbound.xml

import no.nav.helse.base64container.Base64Container
import no.nav.helse.msgHead.XMLDocument
import no.nav.helse.msgHead.XMLMsgHead
import no.nav.helsemelding.inbound.model.Attachment
import java.util.HexFormat

private val PDF_MAGIC_NUMBER: ByteArray = HexFormat.of().parseHex("255044462D")

fun XMLMsgHead.extractValidVedlegg(): List<XMLDocument> =
    this.extractAllVedlegg().filter {
        it.isVedlegg() && it.pdfContentMatchesMimeType()
    }

fun XMLMsgHead.extractAllVedlegg(): List<XMLDocument> {
    val vedlegg = mutableListOf<XMLDocument>()

    vedlegg.addAll(this.extractAllVedleggFromMsgHead())

    this.document.forEach { document ->
        document.refDoc.content.any.forEach {
            if (it is XMLMsgHead) {
                vedlegg.addAll(this.extractAllVedleggFromMsgHead())
            }
        }
    }

    return vedlegg
}

fun XMLMsgHead.extractAllVedleggFromMsgHead(): List<XMLDocument> =
    this.document.filter { it.isVedlegg() }

fun XMLMsgHead.removeVedlegg() {
    document.removeAll { it.isVedlegg() }

    document.forEach { document ->
        document.refDoc.content.any.forEach {
            if (it is XMLMsgHead) {
                it.removeVedlegg()
            }
        }
    }
}

fun XMLDocument.isVedlegg(): Boolean =
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

fun XMLDocument.toVedlegg(): Attachment =
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
