package no.nav.helsemelding.inbound

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import kotlinx.coroutines.flow.Flow
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.client.EdiAdapterError
import no.nav.helsemelding.ediadapter.model.common.GetBusinessDocumentResponse
import no.nav.helsemelding.ediadapter.model.v3.DeleteNotificationsRequest
import no.nav.helsemelding.ediadapter.model.v3.GetMessageResponse
import no.nav.helsemelding.ediadapter.model.v3.GetNotificationsResponse
import no.nav.helsemelding.ediadapter.model.v3.GetStatusResponse
import no.nav.helsemelding.ediadapter.model.v3.GetUnreadNotificationsResponse
import no.nav.helsemelding.ediadapter.model.v3.MarkAsDownloadedRequest
import no.nav.helsemelding.ediadapter.model.v3.Notification
import no.nav.helsemelding.ediadapter.model.v3.PingResponse
import no.nav.helsemelding.ediadapter.model.v3.PostAppRecRequest
import no.nav.helsemelding.ediadapter.model.v3.PostApprecResponse
import no.nav.helsemelding.ediadapter.model.v3.PostMessageRequest
import no.nav.helsemelding.ediadapter.model.v3.PostMessageResponse
import no.nav.helsemelding.ediadapter.model.v3.SetMshConfigurationsRequest
import no.nav.helsemelding.ediadapter.model.v3.UnreadNotification
import kotlin.uuid.Uuid

@Suppress("DEPRECATION", "OVERRIDE_DEPRECATION")
class FakeEdiAdapterClient : EdiAdapterClient {
    var notifications: Flow<Either<EdiAdapterError, UnreadNotification>> = kotlinx.coroutines.flow.emptyFlow()
    var deleteError: EdiAdapterError? = null
    val notificationRequests = mutableListOf<List<Int>>()
    val deleteRequests = mutableListOf<DeleteNotificationsRequest>()
    val documentRequests = mutableListOf<Uuid>()
    val downloadedRequests = mutableListOf<Pair<Uuid, MarkAsDownloadedRequest>>()
    val apprecRequests = mutableListOf<Pair<Uuid, PostAppRecRequest>>()
    var beforeDelete: suspend () -> Unit = {}
    private var documentResponse: Either<EdiAdapterError, GetBusinessDocumentResponse> = unsupported()
    private var downloadedResponse: Either<EdiAdapterError, Unit> = Unit.right()
    private var apprecResponse: Either<EdiAdapterError, PostApprecResponse> = PostApprecResponse(Uuid.random()).right()

    fun givenMarkAsDownloaded(response: Either<EdiAdapterError, Unit>) { downloadedResponse = response }
    fun givenGetBusinessDocumentResponse(response: Either<EdiAdapterError, GetBusinessDocumentResponse>) { documentResponse = response }
    fun givenPostApprecResponse(response: Either<EdiAdapterError, PostApprecResponse>) { apprecResponse = response }

    override fun streamUnreadNotifications(herIds: List<Int>): Flow<Either<EdiAdapterError, UnreadNotification>> {
        notificationRequests += herIds
        return notifications
    }
    override suspend fun deleteNotifications(request: DeleteNotificationsRequest): Either<EdiAdapterError, Unit> {
        beforeDelete()
        deleteRequests += request
        return deleteError?.left() ?: Unit.right()
    }
    override suspend fun getBusinessDocument(id: Uuid): Either<EdiAdapterError, GetBusinessDocumentResponse> {
        documentRequests += id
        return documentResponse
    }
    override suspend fun markMessageAsDownloaded(id: Uuid, request: MarkAsDownloadedRequest): Either<EdiAdapterError, Unit> {
        downloadedRequests += id to request
        return downloadedResponse
    }
    override suspend fun postApprec(id: Uuid, request: PostAppRecRequest): Either<EdiAdapterError, PostApprecResponse> {
        apprecRequests += id to request
        return apprecResponse
    }
    override suspend fun setMshConfigurations(request: SetMshConfigurationsRequest): Either<EdiAdapterError, Unit> = Unit.right()
    override suspend fun getNotifications(herIds: List<Int>, offset: Long, notificationsToFetch: Int?): Either<EdiAdapterError, GetNotificationsResponse> = unsupported()
    override fun streamNotifications(herIds: List<Int>, offset: Long?): Flow<Either<EdiAdapterError, Notification>> = kotlinx.coroutines.flow.emptyFlow()
    override suspend fun getUnreadNotifications(herIds: List<Int>, notificationsToFetch: Int?): Either<EdiAdapterError, GetUnreadNotificationsResponse> = unsupported()
    override suspend fun getMessage(id: Uuid): Either<EdiAdapterError, GetMessageResponse> = unsupported()
    override suspend fun getMessageStatus(id: Uuid): Either<EdiAdapterError, GetStatusResponse> = unsupported()
    override suspend fun postMessage(request: PostMessageRequest): Either<EdiAdapterError, PostMessageResponse> = unsupported()
    override suspend fun deleteMshConfigurations(herIds: List<Int>): Either<EdiAdapterError, Unit> = unsupported()
    override suspend fun ping(): Either<EdiAdapterError, PingResponse> = unsupported()
    override fun close() {}
    private fun <T> unsupported(): Either<EdiAdapterError, T> = EdiAdapterError.Api(404).left()
}
