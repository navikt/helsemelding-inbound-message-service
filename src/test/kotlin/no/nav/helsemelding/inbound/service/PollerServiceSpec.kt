package no.nav.helsemelding.inbound.service

import arrow.core.Either.Right
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.inbound.FakeMessagePublisher
import java.util.Base64
import kotlin.uuid.Uuid

class PollerServiceSpec : StringSpec(
    {
        "message should be processed if it is an apprec" {
            val uuid = Uuid.random()
            val ediAdapterClient = FakeEdiAdapterClient()
            ediAdapterClient.givenMarkAsRead(uuid, Right(true))

            val fakeMessagePublisher = FakeMessagePublisher()
            val pollerService = PollerService(ediAdapterClient, fakeMessagePublisher)

            val message = Message(
                id = uuid,
                isAppRec = true,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe true
        }

        "message should be processed if it's an incoming message" {
            val uuid = Uuid.random()

            val xml = "<dialogmelding>hello</dialogmelding>"
            val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())

            val ediAdapterClient = FakeEdiAdapterClient()
            ediAdapterClient.givenGetBusinessDocumentResponse(
                Right(
                    GetBusinessDocumentResponse(
                        businessDocument = encoded,
                        contentType = "application/xml",
                        contentTransferEncoding = "base64"

                    )
                )
            )

            val publisher = FakeMessagePublisher()

            val pollerService = PollerService(ediAdapterClient, publisher)

            val message = Message(
                id = uuid,
                isAppRec = false,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            val result = pollerService.processMessage(message)

            result shouldBe true
            publisher.publishedKey shouldNotBe null
            publisher.publishedPayload shouldNotBe null

            publisher.publishedKey shouldBe uuid.toString()
            String(publisher.publishedPayload!!) shouldBe xml
        }
    }
)
