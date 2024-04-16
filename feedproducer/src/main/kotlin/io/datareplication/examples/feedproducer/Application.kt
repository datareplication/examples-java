package io.datareplication.examples.feedproducer

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.datareplication.examples.feedproducer.repository.FeedEntityJdbcRepository
import io.datareplication.examples.feedproducer.repository.FeedPageMetadataJdbcRepository
import io.datareplication.examples.feedproducer.repository.FeedProducerJournalJdbcRepository
import io.datareplication.model.Url
import io.datareplication.producer.feed.FeedPageProvider
import io.datareplication.producer.feed.FeedProducer
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.util.Properties
import kotlin.collections.set

fun main(args: Array<String>) {
    val config = ConfigFactory.load()
    val jdbc = run {
        val hikariProps = Properties()
        for (entry in config.getConfig("db.hikari").entrySet()) {
            hikariProps[entry.key] = entry.value.unwrapped()
        }
        val dataSourceProps = Properties()
        for (entry in config.getConfig("db.dataSourceProperties").entrySet()) {
            dataSourceProps[entry.key] = entry.value.unwrapped()
        }
        val dbConfig = HikariConfig(hikariProps)
        dbConfig.dataSourceProperties = dataSourceProps
        val dataSource = HikariDataSource(dbConfig)
        NamedParameterJdbcTemplate(dataSource)
    }

    runBlocking {
        val feedEntityRepository = FeedEntityJdbcRepository(jdbc, this)
        val feedPageMetadataRepository = FeedPageMetadataJdbcRepository(jdbc, this)
        val feedProducerJournalRepository = FeedProducerJournalJdbcRepository(jdbc, this)
        feedEntityRepository.init()
        feedPageMetadataRepository.init()
        feedProducerJournalRepository.init()
        val feedProducer = FeedProducer
            .builder(
                feedEntityRepository,
                feedPageMetadataRepository,
                feedProducerJournalRepository
            )
            .maxBytesPerPage(config.getLong("feed.maxBytesPerPage"))
            .maxEntitiesPerPage(config.getLong("feed.maxEntitiesPerPage"))
            .build()
        val feedBaseUrl = config.getString("feed.base.url")
        val feedPageProvider = FeedPageProvider
            .builder(
                feedEntityRepository,
                feedPageMetadataRepository
            ) { pageId -> Url.of("$feedBaseUrl/feed/${pageId.value()}") }
            .build()

        val env = applicationEngineEnvironment {
            this.config = HoconApplicationConfig(config)
            developmentMode = developmentMode or config.getBoolean("ktor.development")
            connector {
                port = config.getInt("ktor.deployment.port")
            }
            module {
                feedProducerRoutes(feedPageProvider)
            }
        }
        val httpEngine = embeddedServer(Netty, env)
        httpEngine.start(wait = false)
        runProducer(config, feedProducer, this)
        awaitCancellation()
    }
}
