package no.nav.helsemelding.inbound.config

import kotlin.time.Duration

data class Config(
    val server: Server,
    val ediAdapter: EdiAdapter,
    val poller: Poller
) {
    init {
        require(poller.fetchLimit in 1..100, { "fetchLimit must be between 1..100" })
    }
}

data class Server(
    val port: Port,
    val preWait: Duration
)

data class EdiAdapter(
    val scope: Scope
) {
    @JvmInline
    value class Scope(val value: String)
}

data class Poller(
    val herId: Int,
    val fetchLimit: Int,
    val batchSize: Int,
    val scheduleInterval: Duration
)

@JvmInline
value class Port(val value: Int)
