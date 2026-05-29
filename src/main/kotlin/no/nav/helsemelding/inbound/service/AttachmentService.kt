package no.nav.helsemelding.inbound.service

import com.migesok.jaxb.adapter.javatime.LocalDateTimeXmlAdapter
import com.migesok.jaxb.adapter.javatime.LocalDateXmlAdapter
import no.nav.helse.apprecV1.XMLAppRec
import no.nav.helse.base64container.Base64Container
import no.nav.helse.dialogmelding.XMLDialogmelding
import no.nav.helse.eiFellesformat2.XMLEIFellesformat
import no.nav.helse.eiFellesformat2.XMLMottakenhetBlokk
import no.nav.helse.msgHead.XMLDocument
import no.nav.helse.msgHead.XMLMsgHead
import org.xml.sax.InputSource
import java.io.StringReader
import java.text.ParsePosition
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.HexFormat
import javax.xml.bind.DatatypeConverter
import javax.xml.bind.JAXBContext
import javax.xml.bind.Unmarshaller
import javax.xml.parsers.SAXParserFactory
import javax.xml.transform.Source
import javax.xml.transform.sax.SAXSource

class AttachmentService {

    private val PDF_MAGIC_NUMBER: ByteArray = HexFormat.of().parseHex("255044462D")

    val fellesformatJaxBContext: JAXBContext = JAXBContext.newInstance(
        XMLEIFellesformat::class.java,
        XMLMsgHead::class.java,
        XMLMottakenhetBlokk::class.java,
        XMLDialogmelding::class.java,
        Base64Container::class.java,
        XMLAppRec::class.java
    )

    fun splitFellesformatAndVedlegg(fellesformatXml: String): SplitFellesformat {
        val fellesformat = safeUnmarshal(fellesformatXml)

        val vedlegg = extractValidVedlegg(fellesformat)
            .map { it.toVedlegg() }

        fellesformat.removeVedlegg()

        return SplitFellesformat(
            fellesformatWithoutVedlegg = fellesformat,
            vedlegg = vedlegg
        )
    }

    fun safeUnmarshal(inputMessageText: String): XMLEIFellesformat {
        val spf = SAXParserFactory.newInstance()
        spf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        spf.isNamespaceAware = true

        val xmlSource: Source = SAXSource(
            spf.newSAXParser().xmlReader,
            InputSource(StringReader(inputMessageText))
        )

        return getFellesformatUnmarshaller().unmarshal(xmlSource) as XMLEIFellesformat
    }

    private fun extractValidVedlegg(fellesformat: XMLEIFellesformat): List<XMLDocument> =
        extractAllVedlegg(fellesformat).filter {
            it.isVedlegg() && it.pdfContentMatchesMimeType()
        }

    private fun XMLEIFellesformat.removeVedlegg() {
        val outerMsgHead = get<XMLMsgHead>()
        val firstXmlDoc = outerMsgHead.document.firstOrNull { it.refDoc.msgType.v == "XML" }
        val innerMsgHead = firstXmlDoc?.refDoc?.content?.any?.firstOrNull { it is XMLMsgHead } as? XMLMsgHead

        if (innerMsgHead != null) {
            innerMsgHead.document.removeAll { it.isVedlegg() }
        } else {
            outerMsgHead.document.removeAll { it.isVedlegg() }
        }
    }

    private fun extractAllVedlegg(fellesformat: XMLEIFellesformat): List<XMLDocument> {
        val msgHead = fellesformat.get<XMLMsgHead>()
        val vedlegg = mutableListOf<XMLDocument>()

        vedlegg.addAll(extractAllVedleggFromMsgHead(msgHead))

        msgHead.document.forEach { document ->
            document.refDoc.content.any.forEach {
                if (it is XMLMsgHead) {
                    vedlegg.addAll(extractAllVedleggFromMsgHead(it))
                }
            }
        }

        return vedlegg
    }

    private fun getFellesformatUnmarshaller(): Unmarshaller =
        fellesformatJaxBContext.createUnmarshaller().apply {
            setAdapter(LocalDateTimeXmlAdapter::class.java, XMLDateTimeAdapter())
            setAdapter(LocalDateXmlAdapter::class.java, XMLDateAdapter())
        }

    private inline fun <reified T> XMLEIFellesformat.get(): T =
        any.find { it is T } as T

    private fun extractAllVedleggFromMsgHead(msgHead: XMLMsgHead): List<XMLDocument> =
        msgHead.document.filter { it.isVedlegg() }

    private fun XMLDocument.isVedlegg(): Boolean =
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

    private fun XMLDocument.pdfContentMatchesMimeType(): Boolean =
        refDoc.mimeType != "application/pdf" || toBase64Container().value.binaryContentIsPdf()

    private fun XMLDocument.toBase64Container(): Base64Container =
        refDoc.content.any[0] as Base64Container

    private fun ByteArray.binaryContentIsPdf(): Boolean =
        copyOf(PDF_MAGIC_NUMBER.size) contentEquals PDF_MAGIC_NUMBER

    private fun XMLDocument.toVedlegg(): Vedlegg =
        Vedlegg(
            mimeType = refDoc.mimeType,
            beskrivelse = refDoc.description ?: "",
            contentBase64 = toBase64Container().value
        )
}

data class Vedlegg(
    val mimeType: String,
    val beskrivelse: String,
    val contentBase64: ByteArray
)

data class SplitFellesformat(
    val fellesformatWithoutVedlegg: XMLEIFellesformat,
    val vedlegg: List<Vedlegg>
)

class XMLDateTimeAdapter : LocalDateTimeXmlAdapter() {
    override fun unmarshal(stringValue: String?): LocalDateTime? = when (stringValue) {
        null -> null
        else -> {
            if (hasTimeZoneInformation(stringValue)) {
                ZonedDateTime.parse(stringValue)
                    .withZoneSameInstant(ZoneId.of("Europe/Oslo"))
                    .toLocalDateTime()
            } else {
                LocalDateTime.parse(stringValue)
            }
        }
    }
}

fun hasTimeZoneInformation(text: CharSequence): Boolean {
    val position = ParsePosition(0)
    val temporalAccessor = DateTimeFormatter.ISO_ZONED_DATE_TIME.parseUnresolved(text, position)
    return temporalAccessor != null && position.errorIndex < 0 && position.index >= text.length
}

class XMLDateAdapter : LocalDateXmlAdapter() {
    override fun unmarshal(stringValue: String?): LocalDate? = when (stringValue) {
        null -> null
        else -> DatatypeConverter.parseDate(stringValue).toInstant().atZone(ZoneOffset.MAX).toLocalDate()
    }
}
