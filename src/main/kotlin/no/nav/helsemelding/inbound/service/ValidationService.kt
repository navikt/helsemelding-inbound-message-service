package no.nav.helsemelding.inbound.service

import no.nav.helsemelding.messageconverter.model.SplitMessage

fun interface ValidationService {
    fun validate(splitMessage: SplitMessage): Boolean
}

class MessageValidationService : ValidationService {
    override fun validate(splitMessage: SplitMessage): Boolean = true
}
