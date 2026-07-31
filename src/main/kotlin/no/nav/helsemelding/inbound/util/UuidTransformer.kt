package no.nav.helsemelding.inbound.util

import org.jetbrains.exposed.v1.core.ColumnTransformer
import java.util.UUID as JavaUUID
import kotlin.uuid.Uuid as KotlinUUID

class UuidTransformer : ColumnTransformer<JavaUUID, KotlinUUID> {
    override fun unwrap(value: KotlinUUID): JavaUUID = JavaUUID.fromString(value.toString())
    override fun wrap(value: JavaUUID): KotlinUUID = KotlinUUID.parse(value.toString())
}
