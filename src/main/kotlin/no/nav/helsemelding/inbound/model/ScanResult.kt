package no.nav.helsemelding.inbound.model

import kotlinx.serialization.Serializable

@Serializable
data class ScanResult(
    val filename: String,
    val result: ScanStatus,
    val virus: String = "",
    val error: String = ""
)
