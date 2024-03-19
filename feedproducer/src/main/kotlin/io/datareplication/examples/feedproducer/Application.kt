package io.datareplication.examples.feedproducer

import com.typesafe.config.ConfigFactory
import io.datareplication.examples.feedproducer.repository.FeedEntityFileRepository
import io.datareplication.examples.feedproducer.repository.FeedPageMetadataFileRepository
import io.datareplication.examples.feedproducer.repository.FeedProducerJournalFileRepository
import io.datareplication.model.Body
import io.datareplication.model.feed.OperationType
import io.datareplication.producer.feed.FeedProducer
import io.ktor.server.config.HoconApplicationConfig
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlin.io.path.Path
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

suspend fun producerTask(interval: Duration, count: Int, feedProducer: FeedProducer) {
    var total = 0
    while (true) {
        delay(interval)
        for (i in 1..count) {
            total += 1
            val body = Body.fromUtf8("this is example entity ${i}/${count} (no. $total overall)")
            feedProducer.publish(OperationType.PUT, body).await()
        }
    }
}

fun main(args: Array<String>) {
    val config = ConfigFactory.load()
    val dataDir = Path(config.getString("feed.data.dir"))

    val feedProducer = FeedProducer
        .builder(
            FeedEntityFileRepository(dataDir.resolve("entities")),
            FeedPageMetadataFileRepository(dataDir.resolve("pageMetadata")),
            FeedProducerJournalFileRepository(dataDir.resolve("journal"))
        )
        .maxBytesPerPage(config.getLong("feed.maxBytesPerPage"))
        .maxEntitiesPerPage(config.getLong("feed.maxEntitiesPerPage"))
        .build()

    GlobalScope.launch {
        producerTask(
            interval = config.getDuration("feed.publisher.interval").toKotlinDuration(),
            count = config.getInt("feed.publisher.count"),
            feedProducer = feedProducer
        )
    }

    val env = applicationEngineEnvironment {
        this.config = HoconApplicationConfig(config)
        developmentMode = developmentMode or config.getBoolean("ktor.development")
    }
    val httpEngine = embeddedServer(Netty, env)
    httpEngine.start(wait = true)
}
