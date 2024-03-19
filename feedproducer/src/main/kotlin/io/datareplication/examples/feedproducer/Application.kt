package io.datareplication.examples.feedproducer

import com.typesafe.config.ConfigFactory
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

fun main(args: Array<String>) {
    val config = ConfigFactory.load()
    val env = applicationEngineEnvironment {
        this.config = HoconApplicationConfig(config)
        developmentMode = developmentMode or config.getBoolean("ktor.development")
    }
    val httpEngine = embeddedServer(Netty, env)
    httpEngine.start(wait = true)
}
