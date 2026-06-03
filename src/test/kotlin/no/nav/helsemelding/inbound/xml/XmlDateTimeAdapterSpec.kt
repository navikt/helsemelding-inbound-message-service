package no.nav.helsemelding.inbound.xml

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.time.LocalDateTime

class XmlDateTimeAdapterSpec : StringSpec({

    val adapter = XmlDateTimeAdapter()

    "should return null when input is null" {
        adapter.unmarshal(null) shouldBe null
    }

    "should parse local date time without timezone" {
        val result = adapter.unmarshal("2026-06-03T12:34:56")

        result shouldBe LocalDateTime.of(2026, 6, 3, 12, 34, 56)
    }

    "should parse zoned date time" {
        val result = adapter.unmarshal("2026-06-03T12:34:56+02:00[Europe/Oslo]")

        result shouldBe LocalDateTime.of(2026, 6, 3, 12, 34, 56)
    }

    "should convert timezone to Europe Oslo" {
        val result = adapter.unmarshal("2026-06-03T12:34:56Z")

        result shouldBe LocalDateTime.of(2026, 6, 3, 14, 34, 56)
    }
})
