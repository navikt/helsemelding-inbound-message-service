package no.nav.helsemelding.inbound.service

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.model.ApprecInfo
import no.nav.helsemelding.ediadapter.model.ErrorMessage
import no.nav.helsemelding.ediadapter.model.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.GetMessagesRequest
import no.nav.helsemelding.ediadapter.model.Message
import no.nav.helsemelding.ediadapter.model.Metadata
import no.nav.helsemelding.ediadapter.model.PostAppRecRequest
import no.nav.helsemelding.ediadapter.model.PostMessageRequest
import no.nav.helsemelding.ediadapter.model.StatusInfo
import java.util.Base64
import kotlin.uuid.Uuid

const val FAGSYSTEM_HER_ID = 8142519

class LocalEdiAdapterClient : EdiAdapterClient {
    val errorMessage404 = ErrorMessage(
        error = "Not Found",
        errorCode = 404,
        requestId = Uuid.random().toString()
    )

    override suspend fun getMessageStatus(id: Uuid): Either<ErrorMessage, List<StatusInfo>> = Left(errorMessage404)

    override suspend fun getMessage(id: Uuid): Either<ErrorMessage, Message> = Left(errorMessage404)

    override suspend fun getBusinessDocument(id: Uuid): Either<ErrorMessage, GetBusinessDocumentResponse> {
        val xml = this::class.java.classLoader.getResource("message/incomingDialogMessage.xml")!!.readText()
        val encoded = Base64.getEncoder().encodeToString(xml.toByteArray())
        return Right(
            GetBusinessDocumentResponse(
                businessDocument = encoded,
                contentType = "application/xml",
                contentTransferEncoding = "base64"

            )
        )
    }

    override suspend fun postApprec(
        id: Uuid,
        apprecSenderHerId: Int,
        postAppRecRequest: PostAppRecRequest
    ): Either<ErrorMessage, Metadata> = Left(errorMessage404)

    override suspend fun markMessageAsRead(id: Uuid, herId: Int): Either<ErrorMessage, Boolean> = Right(true)

    override suspend fun getApprecInfo(id: Uuid): Either<ErrorMessage, List<ApprecInfo>> = Left(errorMessage404)

    override suspend fun getMessages(getMessagesRequest: GetMessagesRequest): Either<ErrorMessage, List<Message>> {
        val messages = listOf(
            Message(
                id = Uuid.random(),
                receiverHerId = FAGSYSTEM_HER_ID,
                isAppRec = false
            ),
            Message(
                id = Uuid.random(),
                receiverHerId = FAGSYSTEM_HER_ID,
                isAppRec = true
            )
        )
        return Right(messages)
    }

    override suspend fun postMessage(postMessagesRequest: PostMessageRequest): Either<ErrorMessage, Metadata> =
        Left(errorMessage404)

    override fun close() {}
}
