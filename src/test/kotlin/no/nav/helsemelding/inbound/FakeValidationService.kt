package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.service.ValidationService
import no.nav.helsemelding.messageconverter.model.SplitMessage

class FakeValidationService : ValidationService {
    var validatedMessage: SplitMessage? = null

    private var validationResult: Boolean = true

    fun givenValidationResult(result: Boolean) {
        validationResult = result
    }

    override suspend fun validate(splitMessage: SplitMessage): Boolean {
        validatedMessage = splitMessage
        return validationResult
    }
}
