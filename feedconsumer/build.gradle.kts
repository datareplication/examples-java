plugins {
    application
    kotlin("jvm") version "2.1.10"
}

group = "io.datareplication.examples"
version = "1.0-SNAPSHOT"

application {
    mainClass = "io.datareplication.examples.feedproducer.ApplicationKt"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.datareplication:datareplication:1.0.0-rc1")

    implementation("com.typesafe:config:1.4.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.10.1")
    implementation("ch.qos.logback:logback-classic:1.5.18")
}
