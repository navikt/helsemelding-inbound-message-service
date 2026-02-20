package no.nav.helsemelding.inbound.util

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
import kotlin.uuid.Uuid

const val FAGSYSTEM_HER_ID = 8142519

class FakeEdiAdapterClient : EdiAdapterClient {
    private val messageById = mutableMapOf<Uuid, Either<ErrorMessage, Message>>()
    private val postApprecById = mutableMapOf<Uuid, Either<ErrorMessage, Metadata>>()
    private val markAsReadById = mutableMapOf<Uuid, Either<ErrorMessage, Boolean>>()

    val errorMessage404 = ErrorMessage(
        error = "Not Found",
        errorCode = 1000,
        requestId = Uuid.random().toString()
    )

    fun givenMarkAsRead(id: Uuid, isMarked: Either<ErrorMessage, Boolean>) {
        markAsReadById[id] = isMarked
    }

    fun givenPostApprec(
        id: Uuid,
        apprecResponse: Either<ErrorMessage, Metadata>
    ) {
        postApprecById[id] = apprecResponse
    }

    override suspend fun getMessageStatus(id: Uuid): Either<ErrorMessage, List<StatusInfo>> = Right(emptyList())

    override suspend fun getMessage(id: Uuid): Either<ErrorMessage, Message> = Left(errorMessage404)

    override suspend fun getBusinessDocument(id: Uuid): Either<ErrorMessage, GetBusinessDocumentResponse> =
        Left(errorMessage404)

    override suspend fun postApprec(
        id: Uuid,
        apprecSenderHerId: Int,
        postAppRecRequest: PostAppRecRequest
    ): Either<ErrorMessage, Metadata> {
        return postApprecById[id] ?: when (messageById[id]) {
            is Right -> Right(Metadata(Uuid.random(), ""))
            else -> Left(errorMessage404)
        }
    }

    override suspend fun markMessageAsRead(id: Uuid, herId: Int): Either<ErrorMessage, Boolean> =
        markAsReadById[id] ?: Right(true)

    override suspend fun getApprecInfo(id: Uuid): Either<ErrorMessage, List<ApprecInfo>> =
        Right(emptyList())

    override suspend fun getMessages(getMessagesRequest: GetMessagesRequest): Either<ErrorMessage, List<Message>> {
        val messages = List(getMessagesRequest.messagesToFetch) {
            Message(
                id = Uuid.random(),
                receiverHerId = FAGSYSTEM_HER_ID,
                isAppRec = false
            )
        }
        messages.forEach { messageById[it.id!!] = Right(it) }
        return Right(messages)
    }

    override suspend fun postMessage(postMessagesRequest: PostMessageRequest): Either<ErrorMessage, Metadata> =
        Left(errorMessage404)

    override fun close() {}
}
