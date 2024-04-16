package io.datareplication.examples.feedproducer

import com.typesafe.config.Config
import io.datareplication.model.Body
import io.datareplication.model.feed.OperationType
import io.datareplication.producer.feed.FeedProducer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

private val logger = LoggerFactory.getLogger("io.datareplication.examples.feedproducer.ProducerKt")

private suspend fun publisherTask(id: Int, interval: Duration, count: Int, feedProducer: FeedProducer) {
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

private suspend fun assignPagesTask(interval: Duration, feedProducer: FeedProducer) {
    while (true) {
        delay(interval)
        logger.debug("assignPages")
        feedProducer.assignPages().await()
    }
}

suspend fun runProducer(config: Config, feedProducer: FeedProducer, scope: CoroutineScope) {
    for (i in 1..config.getInt("feed.publisher.task.count")) {
        scope.launch {
            publisherTask(
                id = i,
                interval = config.getDuration("feed.publisher.interval").toKotlinDuration(),
                count = config.getInt("feed.publisher.entity.count"),
                feedProducer = feedProducer
            )
        }
    }
    scope.launch {
        assignPagesTask(
            interval = config.getDuration("feed.assignPages.interval").toKotlinDuration(),
            feedProducer = feedProducer
        )
    }
}