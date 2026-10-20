# helsemelding-inbound-message-service

Application responsible for processing incoming messages to NAV.

## Overview

`NotificationService` consumes EDI Adapter V3 unread notifications using
`streamUnreadNotifications`. MSH is configured with notification channel `API` at startup.
Consumption is sequential and cancelled before the EDI Adapter client is closed.

The services share notification ownership:

| Owner | Notification type |
|---|---|
| inbound-message-service | `NEW_MESSAGE`, `REFUSED_MESSAGE` |
| outbound-message-service | `MESSAGE_SENT_STATE_UPDATED`, `MESSAGE_APPREC_INFO_UPDATED`, `MESSAGE_DELIVERY_STATE_UPDATED` |

For `NEW_MESSAGE`, inbound:

1. Fetches and decodes the Base64 XML business document.
2. Extracts the XML message ID and attachments.
3. Validates the split message through `ValidationService`.
4. Saves attachments (if any) using the XML message ID.
5. Publishes XML without attachments to Kafka, keyed by the XML message ID and including the attachment count as Kafka header.
6. Marks the EDI message as downloaded and sends an AppRec.
7. Persists the processing result before deleting the notification.

In production, `MessageValidationService` uses `ClamAvClient` to scan attachments; messages without 
attachments skip scanning. `VIRUS_DETECTED` and `VIRUS_SCAN_FAILED` stop processing before attachment storage, 
Kafka publication, download marking and AppRec. The failure is persisted and counted once, then the notification 
is deleted.

AppRec is currently sent immediately after publication.

For `REFUSED_MESSAGE`, inbound logs a warning with the notification ID and optional EDI message ID,
then deletes the notification. Nothing is persisted. Refused documents are unavailable for download,
so this path does not fetch a document or publish XML.

Processing failures are logged, counted and persisted before the notification is deleted. Consumption
continues with the next notification; failed messages are not retried. Each EDI message has one processing
result, enforced by a unique index on `external_message_id`. Notifications for already registered messages
are deleted without processing the message again, regardless of the stored result.

New-message notifications without a related message ID are logged, counted and deleted. Database failures
prevent deletion because the result could not be recorded. Deletion failures are logged without stopping
consumption. The client reconnects the notification stream on transport failures.

Delivery remains at least once: a crash between Kafka publication and recording the processing result
can cause a duplicate publication. Kafka consumers must tolerate this.

## Configuration

| Property | Description |
|---|---|
| `ediAdapter.receiverHerId` | Receiver HER ID; `RECEIVER_HER_ID`, default `8142519` |
| `ediAdapter.scope` | Azure scope for EDI Adapter |

Polling settings have been removed. Replace any `POLLER_HER_ID` environment override with
`RECEIVER_HER_ID`.

## Local development

For local message processing, replace the notification service's external dependencies in `App.kt`:

```kotlin
val notificationService = NotificationService(
    LocalEdiAdapterClient(),
    LocalMessagePublisher(),
    AttachmentStorageService(LocalAttachmentClient()),
    MsgHeadMessageConverter(),
    FakeMetrics(),
    ExposedMessageRepository(deps.database),
    MessageValidationService(deps.clamAvClient)
)
```
