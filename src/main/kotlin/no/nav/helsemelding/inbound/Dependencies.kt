package no.nav.helsemelding.inbound

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import com.zaxxer.hikari.HikariDataSource
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.attachmentclient.AttachmentClient
import no.nav.helsemelding.attachmentclient.HttpAttachmentClient
import no.nav.helsemelding.ediadapter.client.EdiAdapterClient
import no.nav.helsemelding.ediadapter.client.HttpEdiAdapterClient
import no.nav.helsemelding.ediadapter.client.scopedAuthHttpClient
import no.nav.helsemelding.inbound.config.EdiAdapter
import no.nav.helsemelding.inbound.config.Kafka
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.output.MigrateResult
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import no.nav.helsemelding.inbound.config.Database as DatabaseConfig

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val ediAdapterClient: EdiAdapterClient,
    val kafkaPublisher: KafkaPublisher<String, ByteArray>,
    val attachmentClient: AttachmentClient,
    val database: Database
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed prometheus registry" } }
    }

internal suspend fun ResourceScope.ediAdapterClient(ediAdapter: EdiAdapter): EdiAdapterClient =
    install({ HttpEdiAdapterClient(scopedAuthHttpClient(ediAdapter.scope.value)) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed edi adapter client" } }
    }

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val ediAdapterClient = async { ediAdapterClient(config.ediAdapter) }
    val kafkaPublisher = async { kafkaPublisher(config.kafka) }
    val attachmentClient = async { attachmentClient() }
    val dataSource = async { dataSource(config.database) }
    val database = async { database(config.database, dataSource.await()) }

    Dependencies(
        metricsRegistry.await(),
        ediAdapterClient.await(),
        kafkaPublisher.await(),
        attachmentClient.await(),
        database.await()
    )
}

internal suspend fun ResourceScope.kafkaPublisher(kafka: Kafka): KafkaPublisher<String, ByteArray> =
    install({ KafkaPublisher(kafka.toPublisherSettings()) }) { p, _ ->
        p.close().also { log.info { "Closed Kafka publisher" } }
    }

internal suspend fun ResourceScope.attachmentClient(): AttachmentClient =
    install({ HttpAttachmentClient() }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed attachment client" } }
    }

internal suspend fun ResourceScope.dataSource(config: DatabaseConfig): HikariDataSource =
    install({ HikariDataSource(config.toHikariConfig()) }) { h, _: ExitCase ->
        h.close().also { log.info { "Closed hikari data source" } }
    }

internal suspend fun ResourceScope.database(config: DatabaseConfig, dataSource: HikariDataSource): Database =
    install({ flyway(dataSource, config.flyway).run { Database.connect(dataSource) } }) { d, _: ExitCase ->
        TransactionManager.closeAndUnregister(d).also { log.info { "Closed database" } }
    }

private fun flyway(dataSource: HikariDataSource, flywayConfig: DatabaseConfig.Flyway): MigrateResult =
    Flyway.configure()
        .dataSource(dataSource)
        .locations(flywayConfig.locations)
        .baselineOnMigrate(flywayConfig.baselineOnMigrate)
        .load()
        .migrate()
