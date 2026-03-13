# helsemelding-inbound-message-service

Application responsible for processing incoming messages to NAV

Overview:
- Poll for new messages and process them one by one:
  - If the message is an apprec - mark message as read.
  - If the message is an incoming message - get business document for this message.
    - If the business document is retrieve - decode it and send to Kafka.
      - if the message is successfully sent to Kafka - mark the message as read.

## Local development

Running the application locally:
1. Replace the usage of `ediAdapterClient` with a fake one.
   See [Replacing ediAdapterClient with a fake](#Replacing-ediAdapterClient-with-a-fake) for more details.
2. Run the application (typically by running the `App` class in your IDE).

### Configuration

Relevant configuration for adjusting the frequency of scheduler and how many messages to fetch.

| Property          | Description                                                 | Type |
|-------------------|-------------------------------------------------------------|------|
| herId             | The receiver herId to fetch messages for (NAV)              | Int  |
| fetchLimit        | Number of messages to fetch. Number between 1 and 100       | Int  |
| batchSize         | Number of messages to process per batch                     | Int  |
| scheduleInterval  | How often scheduler should poll for new messages in minutes | Int  |

### Replacing ediAdapterClient with a fake

To run this locally (meaning without actually sending any HTTP requests) change the following in App.kt:
```kotlin
val poller = PollerService(
    deps.ediAdapterClient,
    DialogMessagePublisher(deps.kafkaPublisher)
)
```

to use `FakeEdiAdapterClient` instead:
```kotlin
val poller = PollerService(
    FakeEdiAdapterClient(),
    FakeMessagePublisher()
)
```
