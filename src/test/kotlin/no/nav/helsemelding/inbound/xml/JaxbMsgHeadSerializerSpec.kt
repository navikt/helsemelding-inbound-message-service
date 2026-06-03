package no.nav.helsemelding.inbound.xml

import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import no.nav.helse.msgHead.XMLMsgHead
import java.nio.file.Files
import java.nio.file.Paths

private const val XML_MESSAGE_PATH = "src/test/resources/message_with_attachments.xml"
private const val XML_MESSAGE_WITH_DOCTYPE_PATH = "src/test/resources/message_with_doctype.xml"

class JaxbMsgHeadSerializerSpec : StringSpec({

    val serializer = JaxbMsgHeadSerializer()

    "should deserialize XML message to XMLMsgHead" {
        val messageXml = String(Files.readAllBytes(Paths.get(XML_MESSAGE_PATH)))

        val msgHead = serializer.deserialize(messageXml)

        msgHead shouldNotBe null
        msgHead::class shouldBe XMLMsgHead::class
        msgHead.msgInfo shouldNotBe null
        msgHead.document.size shouldBe 4
    }

    "should serialize XMLMsgHead to XML string" {
        val messageXml = String(Files.readAllBytes(Paths.get(XML_MESSAGE_PATH)))
        val msgHead = serializer.deserialize(messageXml)

        val serializedXml = serializer.serialize(msgHead)

        serializedXml shouldContain "MsgHead"
        serializedXml shouldContain "MsgInfo"
        serializedXml shouldContain "Base64Container"
    }

    "should deserialize serialized XML message" {
        val messageXml = String(Files.readAllBytes(Paths.get(XML_MESSAGE_PATH)))
        val msgHead = serializer.deserialize(messageXml)

        val serializedXml = serializer.serialize(msgHead)
        val deserializedMsgHead = serializer.deserialize(serializedXml)

        deserializedMsgHead.msgInfo shouldNotBe null
        deserializedMsgHead.document.size shouldBe msgHead.document.size
    }

    "should reject XML with doctype declaration" {
        val messageXml = String(Files.readAllBytes(Paths.get(XML_MESSAGE_WITH_DOCTYPE_PATH)))

        shouldThrowAny {
            serializer.deserialize(messageXml)
        }
    }
})
