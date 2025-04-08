package io.datareplication.examples.feedconsumer

import io.datareplication.consumer.feed.FeedConsumer
import io.datareplication.consumer.feed.StartFrom
import io.datareplication.model.Url
import kotlinx.coroutines.reactive.asFlow
import org.reactivestreams.FlowAdapters
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("io.datareplication.examples.feedconsumer.ConsumerKt")

suspend fun runConsumer(
    startFrom: StartFrom,
    url: Url,

    consumer: FeedConsumer,
    callback: (StartFrom) -> Unit
) {
    FlowAdapters
        .toPublisher(consumer.streamEntities(url, startFrom))
        .asFlow()
        .collect { entity ->
            logger.debug(entity.header().operationType().name + ": " + entity.body().toUtf8())
            callback(StartFrom.contentId(entity.header().contentId(), entity.header().lastModified()))
        }
    logger.debug("Processing completed, wait 1 second")
}