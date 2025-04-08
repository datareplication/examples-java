plugins {
    application
    kotlin("jvm") version "2.1.10"
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
    implementation("io.datareplication:datareplication:1.0.0")

    implementation(platform("io.ktor:ktor-bom:3.0.3"))
    implementation("io.ktor:ktor-server-core-jvm")
    implementation("io.ktor:ktor-server-netty-jvm")
    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("com.typesafe:config:1.4.3")
    implementation("com.zaxxer:HikariCP:6.2.1")
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("org.xerial:sqlite-jdbc:3.45.2.0")
    implementation("org.springframework:spring-jdbc:6.1.5")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
}
