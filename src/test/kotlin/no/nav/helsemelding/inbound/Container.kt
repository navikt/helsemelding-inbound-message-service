package no.nav.helsemelding.inbound

import arrow.core.memoize
import arrow.fx.coroutines.ResourceScope
import org.jetbrains.exposed.v1.jdbc.Database
import org.testcontainers.containers.PostgreSQLContainer
import no.nav.helsemelding.inbound.config.Database as DatabaseConfig

suspend fun ResourceScope.database(jdbcUrl: String): Database =
    database(
        config().database,
        dataSource(config().database.copy(url = DatabaseConfig.Url(jdbcUrl)))
    )

val container: () -> PostgreSQLContainer<Nothing> = {
    PostgreSQLContainer<Nothing>("postgres:18-alpine")
        .apply {
            startupAttempts = 1
            withDatabaseName("inbound-message-service-db")
            withUsername("postgres")
            withPassword("postgres")
        }
}
    .memoize()
