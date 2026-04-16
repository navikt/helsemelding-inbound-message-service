package no.nav.helsemelding.inbound.util

import kotlin.system.measureNanoTime

suspend fun <T> registerDuration(
    registratorFunction: (Long) -> Unit,
    block: suspend () -> T
): T {
    var result: T
    val durationNanos = measureNanoTime {
        result = block()
    }
    registratorFunction(durationNanos)
    return result
}
