package no.nav.helsemelding.inbound.model

import kotlinx.serialization.Serializable

@Serializable
enum class ScanStatus {
    FOUND, OK, ERROR
}
