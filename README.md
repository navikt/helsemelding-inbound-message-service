# helsemelding-inbound-message-service

Application responsible for processing incoming messages to NAV.

## Overview

`NotificationService` consumes EDI Adapter V3 unread notifications using
`streamUnreadNotifications`. MSH is configured with notification channel `API` at startup.
Consumption is sequential and cancelled before the EDI Adapter client is closed.

The services share notification ownership:

| Notification type | Owner |
|---|---|
| `NEW_MESSAGE`, `REFUSED_MESSAGE` | inbound-message-service |
| `MESSAGE_SENT_STATE_UPDATED`, `MESSAGE_APPREC_INFO_UPDATED`, `MESSAGE_DELIVERY_STATE_UPDATED` | outbound-message-service |

For `NEW_MESSAGE`, inbound:

1. Fetches and decodes the Base64 XML business document.
2. Extracts the XML message ID and attachments.
3. Saves attachments using the XML message ID.
4. Publishes XML without attachments to Kafka, keyed by the XML message ID and including the attachment count.
5. Marks the EDI message as downloaded and sends an AppRec.
6. Persists the processing result before deleting the notification.

AppRec is currently sent immediately after publication. It should eventually be sent after feedback
from fagsystem.

For `REFUSED_MESSAGE`, inbound persists the notification ID, optional EDI message ID, receiver HER ID,
description and timestamp in `refused_messages`, then deletes the notification. Refused documents are
unavailable for download, so this path does not fetch a document or publish XML.

Failed processing leaves the notification undeleted and stops consumption, allowing it to be retried
after application restart. Transport failures in the stream are reconnected by the client. Deletion
failures are logged without stopping consumption. Replayed successful messages only retry notification
deletion; recorded download/AppRec failures resume those steps without republishing to Kafka.
Refusal persistence is idempotent by notification ID.

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
    ExposedMessageRepository(deps.database)
)
```
