plugins {
    kotlin("jvm") version "1.9.23"
}

group = "io.datareplication"
version = "1.0-SNAPSHOT"

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
}
