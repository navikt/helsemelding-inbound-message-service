package no.nav.helsemelding.inbound.service

import arrow.core.Either
import arrow.core.Either.Left
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
import kotlin.uuid.Uuid

class FakeEdiAdapterClient : EdiAdapterClient {
    private var getBusinessDocumentResponse: Either<ErrorMessage, GetBusinessDocumentResponse>? = null
    private var markMessageAsReadResponse: Either<ErrorMessage, Boolean>? = null
    private var postApprecResponse: Either<ErrorMessage, Metadata>? = null

    val errorMessage404 = ErrorMessage(
        error = "Not Found",
        errorCode = 404,
        requestId = Uuid.random().toString()
    )

    fun givenMarkAsRead(isMarked: Either<ErrorMessage, Boolean>) {
        markMessageAsReadResponse = isMarked
    }

    fun givenGetBusinessDocumentResponse(response: Either<ErrorMessage, GetBusinessDocumentResponse>) {
        getBusinessDocumentResponse = response
    }

    fun givenPostApprecResponse(response: Either<ErrorMessage, Metadata>) {
        postApprecResponse = response
    }

    override suspend fun getMessageStatus(id: Uuid): Either<ErrorMessage, List<StatusInfo>> = Left(errorMessage404)

    override suspend fun getMessage(id: Uuid): Either<ErrorMessage, Message> = Left(errorMessage404)

    override suspend fun getBusinessDocument(id: Uuid): Either<ErrorMessage, GetBusinessDocumentResponse> = getBusinessDocumentResponse!!

    override suspend fun postApprec(
        id: Uuid,
        apprecSenderHerId: Int,
        postAppRecRequest: PostAppRecRequest
    ): Either<ErrorMessage, Metadata> = postApprecResponse!!

    override suspend fun markMessageAsRead(id: Uuid, herId: Int): Either<ErrorMessage, Boolean> = markMessageAsReadResponse!!

    override suspend fun getApprecInfo(id: Uuid): Either<ErrorMessage, List<ApprecInfo>> = Left(errorMessage404)

    override suspend fun getMessages(getMessagesRequest: GetMessagesRequest): Either<ErrorMessage, List<Message>> =
        Left(errorMessage404)

    override suspend fun postMessage(postMessagesRequest: PostMessageRequest): Either<ErrorMessage, Metadata> =
        Left(errorMessage404)

    override fun close() {}
}
