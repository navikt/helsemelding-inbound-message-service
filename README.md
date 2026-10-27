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
2. Calculates the content hash using `MsgHeadMessageConverter.calculateContentHash`, then extracts the XML message ID and attachments.
3. Validates the split message through `ValidationService`:
   - Checks for duplicate content.
   - Scans attachments for viruses.
4. Saves attachments (if any) using the XML message ID.
5. Publishes XML without attachments to Kafka, keyed by the XML message ID and including the attachment count as Kafka header.
6. Marks the EDI message as downloaded and sends an AppRec.
7. Persists the processing result and, for `SUCCESS` or `DUPLICATE_MESSAGE_DETECTED`, the content hash before deleting the notification.

AppRec is currently sent immediately after publication.

For `REFUSED_MESSAGE`, inbound logs a warning with the notification ID and optional EDI message ID,
then deletes the notification. Nothing is persisted. Refused documents are unavailable for download,
so this path does not fetch a document or publish XML.

New-message notifications without a related message ID are logged, counted and deleted. Database failures
prevent deletion because the result could not be recorded. Deletion failures are logged without stopping
consumption. The client reconnects the notification stream on transport failures.

Delivery remains at least once: a crash between Kafka publication and recording the processing result
can cause a duplicate publication. Kafka consumers must tolerate this.

## Processing results

Processing stops at the first failure. The table follows the processing order.
Failures are logged and counted, and consumption continues with the next notification.
Failed messages are not retried.

| Result | When it occurs |
|---|---|
| `RETRIEVING_BUSINESS_DOCUMENT_FAILED` | The business document could not be retrieved from the EDI Adapter. |
| `DECODING_BUSINESS_DOCUMENT_FAILED` | The document was retrieved, but `businessDocument` cannot be Base64-decoded, for example because it contains invalid Base64 characters. |
| `IGNORED_APPREC` | The document is valid XML whose root element has `localName == "AppRec"`. This result is not persisted, but the notification is deleted. |
| `EXTRACTING_MESSAGE_ID_FAILED` | The converter cannot extract the XML message ID, for example because the XML is malformed or the ID is not a valid UUID. |
| `CALCULATING_CONTENT_HASH_FAILED` | The message ID was extracted, but calculating message hash failed. |
| `SPLITTING_MESSAGE_FAILED` | En error occured while separating the XML from its attachments. |
| `DUPLICATE_MESSAGE_DETECTED` | A message with the same `contentHash` already exists in the database. |
| `VIRUS_SCAN_FAILED` | Attachments exist, but scanning cannot complete or its response cannot be interpreted. |
| `VIRUS_DETECTED` | Scanning returns a non-empty result list with at least one status other than `OK`. |
| `SAVING_ATTACHMENTS_FAILED` | Attachments exist and validation passed, but saving attachments failed. |
| `PUBLISHING_TO_KAFKA_FAILED` | Publishing the XML without attachments to Kafka returns a failed `Result`. Any attachments have already been stored. |
| `MARKING_MESSAGE_AS_DOWNLOADED_FAILED` | Kafka publication succeeded, but the EDI Adapter returns an error when marking the message as downloaded. |
| `SENDING_APPREC_FAILED` | The message was published and marked as downloaded, but the EDI Adapter returns an error when sending an AppRec. |
| `SUCCESS` | Retrieval, decoding, hash calculation, ID extraction, splitting, validation, attachment storage (if needed), Kafka publication, download marking and AppRec sending all succeed. This does not mean the receiving domain system has finished processing the message. |

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
val messageRepository = ExposedMessageRepository(deps.database)

val notificationService = NotificationService(
    LocalEdiAdapterClient(),
    LocalMessagePublisher(),
    AttachmentStorageService(LocalAttachmentClient()),
    MsgHeadMessageConverter(),
    FakeMetrics(),
    ExposedMessageRepository(deps.database),
    MessageValidationService(
        deps.clamAvClient,
        ExposedMessageRepository(deps.database)
    )
)
```
