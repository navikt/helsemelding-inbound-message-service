package no.nav.helsemelding.inbound.config

import kotlin.time.Duration

data class Config(
    val server: Server
)

data class Server(
    val port: Port,
    val preWait: Duration
)

@JvmInline
value class Port(val value: Int)
