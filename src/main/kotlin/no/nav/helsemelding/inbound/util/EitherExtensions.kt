package no.nav.helsemelding.inbound.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right

fun <T> Result<T>.toEither(): Either<Throwable, T> =
    fold(
        onSuccess = { it.right() },
        onFailure = { it.left() }
    )
