package no.nav.helsemelding.inbound.xml

import com.migesok.jaxb.adapter.javatime.LocalDateTimeXmlAdapter
import java.text.ParsePosition
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class XmlDateTimeAdapter : LocalDateTimeXmlAdapter() {
    override fun unmarshal(stringValue: String?): LocalDateTime? = when (stringValue) {
        null -> null
        else -> {
            if (hasTimeZoneInformation(stringValue)) {
                ZonedDateTime.parse(stringValue)
                    .withZoneSameInstant(ZoneId.of("Europe/Oslo"))
                    .toLocalDateTime()
            } else {
                LocalDateTime.parse(stringValue)
            }
        }
    }
}

private fun hasTimeZoneInformation(text: CharSequence): Boolean {
    val position = ParsePosition(0)
    val temporalAccessor = DateTimeFormatter.ISO_ZONED_DATE_TIME.parseUnresolved(text, position)
    return temporalAccessor != null && position.errorIndex < 0 && position.index >= text.length
}
