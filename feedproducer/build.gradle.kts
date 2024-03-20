plugins {
    application
    kotlin("jvm") version "1.9.23"
    kotlin("plugin.serialization") version "1.9.23"
}

group = "io.datareplication.examples"
version = "1.0-SNAPSHOT"

application {
    mainClass = "io.datareplication.examples.feedproducer.ApplicationKt"
    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenCentral()
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/datareplication/datareplication-java")
        credentials {
            username = project.findProperty("datareplication.username") as String?
            password = findProperty("datareplication.password") as String?
        }
    }
}

dependencies {
    implementation("io.datareplication:datareplication:0.0.1-SNAPSHOT")

    implementation(platform("io.ktor:ktor-bom:2.3.9"))
    implementation("io.ktor:ktor-server-core-jvm")
    implementation("io.ktor:ktor-server-netty-jvm")
    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("com.typesafe:config:1.4.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
}
