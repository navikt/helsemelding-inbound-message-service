package no.nav.helsemelding.inbound.xml

import com.migesok.jaxb.adapter.javatime.LocalDateTimeXmlAdapter
import com.migesok.jaxb.adapter.javatime.LocalDateXmlAdapter
import no.nav.helse.apprecV1.XMLAppRec
import no.nav.helse.base64container.Base64Container
import no.nav.helse.dialogmelding.XMLDialogmelding
import no.nav.helse.msgHead.XMLMsgHead
import org.xml.sax.InputSource
import java.io.StringReader
import java.io.StringWriter
import javax.xml.bind.JAXBContext
import javax.xml.bind.Marshaller
import javax.xml.bind.Unmarshaller
import javax.xml.parsers.SAXParserFactory
import javax.xml.transform.Source
import javax.xml.transform.sax.SAXSource

interface MsgHeadSerializer {
    fun deserialize(inputMessageXML: String): XMLMsgHead
    fun serialize(msgHead: XMLMsgHead): String
}

class JaxbMsgHeadSerializer : MsgHeadSerializer {
    private val msgHeadJaxBContext: JAXBContext = JAXBContext.newInstance(
        XMLMsgHead::class.java,
        XMLDialogmelding::class.java,
        Base64Container::class.java,
        XMLAppRec::class.java
    )

    override fun deserialize(inputMessageXML: String): XMLMsgHead {
        val spf = configureParserFactory()
        val unmarshaller = configureUnmarshaller()

        val xmlSource: Source = SAXSource(
            spf.newSAXParser().xmlReader,
            InputSource(StringReader(inputMessageXML))
        )

        return unmarshaller.unmarshal(xmlSource) as XMLMsgHead
    }

    override fun serialize(msgHead: XMLMsgHead): String {
        val stringWriter = StringWriter()
        val marshaller = configureMarshaller()

        marshaller.marshal(msgHead, stringWriter)

        return stringWriter.toString()
    }

    private fun configureUnmarshaller(): Unmarshaller =
        msgHeadJaxBContext.createUnmarshaller().apply {
            setAdapter(LocalDateTimeXmlAdapter::class.java, XmlDateTimeAdapter())
            setAdapter(LocalDateXmlAdapter::class.java, XmlDateAdapter())
        }

    private fun configureMarshaller(): Marshaller =
        msgHeadJaxBContext.createMarshaller().apply {
            setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true)
        }

    private fun configureParserFactory(): SAXParserFactory {
        val parserFactory = SAXParserFactory.newInstance()
        parserFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        parserFactory.isNamespaceAware = true

        return parserFactory
    }
}
