package io.datareplication.examples.feedconsumer

import com.typesafe.config.ConfigFactory
import io.datareplication.consumer.feed.FeedConsumer
import io.datareplication.consumer.feed.StartFrom
import io.datareplication.model.Url
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("io.datareplication.examples.feedconsumer.Application")

fun main() {
    val config = ConfigFactory.load()
    val consumer = FeedConsumer
        .builder()
        .build()
    val url = Url.of(config.getString("feed.base.url"))
    var startFrom: StartFrom = StartFrom.beginning()
    runBlocking {
        while (true) {
            logStartFrom(startFrom)
            try {
                runConsumer(startFrom, url, consumer, callback = { startFrom = it })
            } catch (e: Exception) {
                logger.error(e.toString())
            }

            Thread.sleep(config.getLong("feed.consumer.interval"))
        }
    }
}

private fun logStartFrom(startFrom: StartFrom) {
    when (startFrom) {
        is StartFrom.Beginning ->
            logger.debug("Starting from beginning")

        is StartFrom.Timestamp ->
            logger.debug("Starting from timestamp: {}", startFrom.timestamp())

        is StartFrom.ContentId ->
            logger.debug("Starting from contentId: {} timestamp: {}", startFrom.contentId(), startFrom.timestamp())
    }
}