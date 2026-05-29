import org.gradle.api.file.DuplicatesStrategy.INCLUDE
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    application
    kotlin("jvm") version "2.3.0"
    kotlin("plugin.serialization") version "2.1.10"
    id("io.ktor.plugin") version "3.0.3"
    id("org.jlleitschuh.gradle.ktlint") version "11.6.1"
    id("com.gradleup.shadow") version "9.2.2"
}

tasks {
    shadowJar {
        mergeServiceFiles()
        duplicatesStrategy = INCLUDE
        archiveFileName.set("app.jar")
    }
    test {
        useJUnitPlatform()
    }
    ktlintFormat {
        enabled = true
    }
    ktlintCheck {
        dependsOn("ktlintFormat")
    }
    build {
        dependsOn("ktlintCheck")
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().all {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_21
        freeCompilerArgs = listOf(
            "-opt-in=kotlin.time.ExperimentalTime",
            "-opt-in=kotlin.uuid.ExperimentalUuidApi,arrow.fx.coroutines.await.ExperimentalAwaitAllApi"
        )
    }
}

dependencies {
    implementation(libs.arrow.core)
    implementation(libs.arrow.functions)
    implementation(libs.arrow.fx.coroutines)
    implementation(libs.arrow.resilience)
    implementation(libs.arrow.suspendapp)
    implementation(libs.arrow.suspendapp.ktor)
    implementation(libs.bundles.logging)
    implementation(libs.bundles.prometheus)
    implementation(libs.bundles.opentelemetry)
    implementation(libs.hoplite.core)
    implementation(libs.hoplite.hocon)
    implementation(libs.jwt)
    implementation(libs.nimbus.jwt)
    implementation(libs.ktor.client.auth)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.logging)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.auth.jvm)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.kotlin.logging)
    implementation(libs.token.validation.ktor.v3)
    implementation(libs.kotlin.kafka)
    implementation(libs.edi.adapter.client)

    implementation("javax.xml.bind:jaxb-api:2.4.0-b180830.0359")
    implementation("com.migesok:jaxb-java-time-adapters:1.1.3")

    implementation("no.nav.helse.xml:xmlfellesformat2:1.0329dd1")
    implementation("no.nav.helse.xml:kith-hodemelding:2019.07.30-12-26-5c924ef4f04022bbb850aaf299eb8e4464c1ca6a")
    implementation("no.nav.helse.xml:dialogmelding:1.5d21db9")
    implementation("no.nav.helse.xml:base64Container:1.5ac2176")
    implementation("no.nav.helse.xml:kith-apprec:2019.07.30-04-23-2a0d1388209441ec05d2e92a821eed4f796a3ae2")

    testImplementation(testLibs.bundles.kotest)
    testImplementation(testLibs.kotest.assertions.arrow)
    testImplementation(testLibs.kotest.extensions.jvm)
    testImplementation(testLibs.kotest.extensions.testcontainers)
    testImplementation(testLibs.ktor.server.test.host)
    testImplementation(testLibs.ktor.client.mock)
    testImplementation(testLibs.testcontainers)
    testImplementation(testLibs.testcontainers.postgresql)
    testImplementation(testLibs.turbine)
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("no.nav.helsemelding.inbound.AppKt")
}
