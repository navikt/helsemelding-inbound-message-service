package no.nav.helsemelding.inbound.config

import com.sksamuel.hoplite.Masked
import io.github.nomisRev.kafka.publisher.PublisherSettings
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import kotlin.time.Duration

data class Config(
    val server: Server,
    val ediAdapter: EdiAdapter,
    val poller: Poller,
    val kafka: Kafka
) {
    init {
        require(poller.fetchLimit in 1..100, { "fetchLimit must be between 1..100" })
    }
}

data class Server(
    val port: Port,
    val preWait: Duration
)

data class EdiAdapter(
    val scope: Scope
) {
    @JvmInline
    value class Scope(val value: String)
}

data class Poller(
    val herId: Int,
    val fetchLimit: Int,
    val batchSize: Int,
    val scheduleInterval: Duration
)

data class Kafka(
    val groupId: String,
    val bootstrapServers: String,
    val securityProtocol: SecurityProtocol,
    val keystoreType: KeystoreType,
    val keystoreLocation: KeystoreLocation,
    val keystorePassword: Masked,
    val truststoreType: TruststoreType,
    val truststoreLocation: TruststoreLocation,
    val truststorePassword: Masked,
    val topics: Topics
) {
    private val securityProtocolConfig = "security.protocol"
    private val sslKeystoreTypeConfig = "ssl.keystore.type"
    private val sslKeystoreLocationConfig = "ssl.keystore.location"
    private val sslKeystorePasswordConfig = "ssl.keystore.password"
    private val sslTruststoreTypeConfig = "ssl.truststore.type"
    private val sslTruststoreLocationConfig = "ssl.truststore.location"
    private val sslTruststorePasswordConfig = "ssl.truststore.password"

    @JvmInline
    value class SecurityProtocol(val value: String)

    @JvmInline
    value class KeystoreType(val value: String)

    @JvmInline
    value class KeystoreLocation(val value: String)

    @JvmInline
    value class TruststoreType(val value: String)

    @JvmInline
    value class TruststoreLocation(val value: String)

    fun toPublisherSettings(): PublisherSettings<String, ByteArray> =
        PublisherSettings(
            bootstrapServers = bootstrapServers,
            keySerializer = StringSerializer(),
            valueSerializer = ByteArraySerializer(),
            properties = toProperties()
        )

    private fun toProperties() = Properties()
        .apply {
            put(securityProtocolConfig, securityProtocol.value)
            put(sslKeystoreTypeConfig, keystoreType.value)
            put(sslKeystoreLocationConfig, keystoreLocation.value)
            put(sslKeystorePasswordConfig, keystorePassword.value)
            put(sslTruststoreTypeConfig, truststoreType.value)
            put(sslTruststoreLocationConfig, truststoreLocation.value)
            put(sslTruststorePasswordConfig, truststorePassword.value)
        }
}

data class Topics(
    val dialogMessage: String
)

@JvmInline
value class Port(val value: Int)
