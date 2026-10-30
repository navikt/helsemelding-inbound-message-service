package no.nav.helsemelding.inbound

import no.nav.helsemelding.inbound.model.ProcessingResult
import no.nav.helsemelding.inbound.service.ValidationService
import no.nav.helsemelding.messageconverter.model.SplitMessage
import kotlin.uuid.Uuid

class FakeValidationService : ValidationService {
    var validatedMessage: SplitMessage? = null
    var validatedContentHash: String? = null

    private var validationResult: ProcessingResult = ProcessingResult.SUCCESS

    fun givenValidationResult(result: ProcessingResult) {
        validationResult = result
    }

    override suspend fun validate(splitMessage: SplitMessage, messageId: Uuid, contentHash: String?): ProcessingResult {
        validatedMessage = splitMessage
        validatedContentHash = contentHash
        return validationResult
    }
}
