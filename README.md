# helsemelding-inbound-message-service

Application responsible for processing incoming messages to NAV

## Overview

The service polls EDI Adapter for incoming messages for a configured HER-id.

Each message is processed depending on its type:

1. If the message is an `AppRec`, the service marks it as read using EDI Adapter.
2. If the message is a normal incoming message, the service:
   - fetches the business document using EDI Adapter.
   - decodes the Base64 encoded XML payload,
   - deserializes the `MsgHead` XML,
   - extracts attachments from the message,
   - removes the attachments from the `MsgHead` XML,
   - saves the extracted attachments using Attachment Service,
   - publishes the `MsgHead` XML without attachments to Kafka,
   - marks the original message as read using EDI Adapter,
   - sends an `AppRec` to NHN using EDI Adapter.

### Configuration

Relevant configuration for adjusting the frequency of scheduler and how many messages to fetch.

| Property          | Description                                                 | Type |
|-------------------|-------------------------------------------------------------|------|
| herId             | The receiver herId to fetch messages for (NAV)              | Int  |
| fetchLimit        | Number of messages to fetch. Number between 1 and 100       | Int  |
| batchSize         | Number of messages to process per batch                     | Int  |
| scheduleInterval  | How often scheduler should poll for new messages in minutes | Int  |

### Local development

To run this service locally (without integration with Kafka and other services) change the following in App.kt:
```kotlin
val poller = PollerService(
    deps.ediAdapterClient,
    dialogMessagePublisher,
    attachmentService,
    metrics
)
```

with local implementations of the dependencies:
```kotlin
val poller = PollerService(
    LocalEdiAdapterClient(),
    LocalMessagePublisher(),
    DomAttachmentService(
        JaxbMsgHeadSerializer(),
        LocalAttachmentClient()
    ),
    FakeMetrics()
)
```
