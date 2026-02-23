package no.nav.helsemelding.inbound.service

import arrow.core.Either
import arrow.core.Either.Right
import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.row
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.inbound.util.FAGSYSTEM_HER_ID
import no.nav.helsemelding.inbound.util.FakeEdiAdapterClient
import kotlin.uuid.Uuid

class PollerServiceSpec : StringSpec(
    {
        "message should be processed if it is an apprec" {
            val uuid = Uuid.random()
            val ediAdapterClient = FakeEdiAdapterClient()
            ediAdapterClient.givenMarkAsRead(uuid, Right(true))
            val pollerService = PollerService(ediAdapterClient)

            val message = Message(
                id = uuid,
                isAppRec = true,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe true
        }

        "message should not be processed if it's not an apprec" {
            forAll(
                row(null),
                row(false)
            ) { isApprec ->
                val uuid = Uuid.random()
                val ediAdapterClient = FakeEdiAdapterClient()
                ediAdapterClient.givenMarkAsRead(uuid, Right(true))
                val pollerService = PollerService(ediAdapterClient)

                val message = Message(
                    id = uuid,
                    isAppRec = isApprec,
                    receiverHerId = FAGSYSTEM_HER_ID
                )

                pollerService.processMessage(message) shouldBe false
            }
        }

        "message should not be processed if ediAdapterClient returns error" {
            val uuid = Uuid.random()
            val ediAdapterClient = FakeEdiAdapterClient()

            val errorMessage500 = ErrorMessage(
                error = "Internal Server Error",
                errorCode = 1000,
                requestId = Uuid.random().toString()
            )
            ediAdapterClient.givenMarkAsRead(uuid, Either.Left(errorMessage500))
            val pollerService = PollerService(ediAdapterClient)

            val message = Message(
                id = uuid,
                isAppRec = true,
                receiverHerId = FAGSYSTEM_HER_ID
            )

            pollerService.processMessage(message) shouldBe false
        }
    }
)
