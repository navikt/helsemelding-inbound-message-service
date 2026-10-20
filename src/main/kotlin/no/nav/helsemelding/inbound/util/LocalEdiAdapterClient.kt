package no.nav.helsemelding.inbound.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
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
import no.nav.helsemelding.ediadapter.model.v3.NotificationType
import no.nav.helsemelding.ediadapter.model.v3.PingResponse
import no.nav.helsemelding.ediadapter.model.v3.PostAppRecRequest
import no.nav.helsemelding.ediadapter.model.v3.PostApprecResponse
import no.nav.helsemelding.ediadapter.model.v3.PostMessageRequest
import no.nav.helsemelding.ediadapter.model.v3.PostMessageResponse
import no.nav.helsemelding.ediadapter.model.v3.SetMshConfigurationsRequest
import no.nav.helsemelding.ediadapter.model.v3.UnreadNotification
import java.util.Base64
import kotlin.uuid.Uuid

@Suppress("DEPRECATION", "OVERRIDE_DEPRECATION")
class LocalEdiAdapterClient : EdiAdapterClient {
    override fun streamUnreadNotifications(herIds: List<Int>): Flow<Either<EdiAdapterError, UnreadNotification>> = flow {
        while (true) {
            for (herId in herIds) {
                emit(UnreadNotification(Uuid.random(), Uuid.random(), NotificationType.NEW_MESSAGE, herId).right())
            }
            delay(2000)
        }
    }
    override suspend fun getBusinessDocument(id: Uuid): Either<EdiAdapterError, GetBusinessDocumentResponse> {
        val xml = javaClass.classLoader.getResource("message/incomingDialogMessage.xml")!!.readText()
        return GetBusinessDocumentResponse(Base64.getEncoder().encodeToString(xml.toByteArray()), "application/xml", "base64").right()
    }
    override suspend fun markMessageAsDownloaded(id: Uuid, request: MarkAsDownloadedRequest): Either<EdiAdapterError, Unit> = Unit.right()
    override suspend fun postApprec(id: Uuid, request: PostAppRecRequest): Either<EdiAdapterError, PostApprecResponse> = PostApprecResponse(Uuid.random()).right()
    override suspend fun deleteNotifications(request: DeleteNotificationsRequest): Either<EdiAdapterError, Unit> = Unit.right()
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
