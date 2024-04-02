package io.datareplication.examples.feedproducer

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.datareplication.examples.feedproducer.repository.FeedEntityJdbcRepository
import io.datareplication.examples.feedproducer.repository.FeedPageMetadataJdbcRepository
import io.datareplication.examples.feedproducer.repository.FeedProducerJournalJdbcRepository
import io.datareplication.model.Body
import io.datareplication.model.feed.OperationType
import io.datareplication.producer.feed.FeedProducer
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import kotlin.io.path.Path
import kotlin.io.path.createDirectories
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

private val logger = LoggerFactory.getLogger("io.datareplication.examples.feedproducer.Application")

suspend fun publisherTask(id: Int, interval: Duration, count: Int, feedProducer: FeedProducer) {
    var total = 0
    while (true) {
        delay(interval)
        logger.debug("publishing $count more entities")
        for (i in 1..count) {
            total += 1
            val body = Body.fromUtf8("this is example entity $total by publisher $id")
            feedProducer.publish(OperationType.PUT, body).await()
        }
    }
}

suspend fun assignPagesTask(interval: Duration, feedProducer: FeedProducer) {
    while (true) {
        delay(interval)
        logger.debug("assignPages")
        feedProducer.assignPages().await()
    }
}

fun main(args: Array<String>) {
    val config = ConfigFactory.load()
    Path("./data").createDirectories()
    val hikariConfig = HikariConfig().apply {
        jdbcUrl = config.getString("db.url")
        maximumPoolSize = 1
        connectionTestQuery = "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;"
        driverClassName = "org.sqlite.JDBC"
    }
    val dataSource = HikariDataSource(hikariConfig)
    val jdbc = NamedParameterJdbcTemplate(dataSource)

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

        for (i in 1..config.getInt("feed.publisher.task.count")) {
            launch {
                publisherTask(
                    id = i,
                    interval = config.getDuration("feed.publisher.interval").toKotlinDuration(),
                    count = config.getInt("feed.publisher.entity.count"),
                    feedProducer = feedProducer
                )
            }
        }
        launch {
            assignPagesTask(
                interval = config.getDuration("feed.assignPages.interval").toKotlinDuration(),
                feedProducer = feedProducer
            )
        }

        val env = applicationEngineEnvironment {
            this.config = HoconApplicationConfig(config)
            developmentMode = developmentMode or config.getBoolean("ktor.development")
            connector {
                port = config.getInt("ktor.deployment.port")
            }
        }
        val httpEngine = embeddedServer(Netty, env)
        httpEngine.start(wait = false)
        awaitCancellation()
    }
}
