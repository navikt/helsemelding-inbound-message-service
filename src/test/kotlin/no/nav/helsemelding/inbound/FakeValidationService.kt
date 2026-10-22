package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.ValidationResult
import no.nav.helsemelding.inbound.service.ValidationService
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

class FakeValidationService : ValidationService {
    var validatedMessage: SplitMessage? = null

    private var validationResult: ValidationResult = ValidationResult.OK

    fun givenValidationResult(result: ValidationResult) {
        validationResult = result
    }

    override suspend fun validate(splitMessage: SplitMessage, messageId: Uuid): ValidationResult {
        validatedMessage = splitMessage
        return validationResult
    }
}
