package io.datareplication.examples.feedproducer.repository

import org.slf4j.Logger
import kotlin.time.measureTimedValue

internal inline fun <T> timed(logger: Logger, name: String, crossinline f: () -> T): T {
    logger.trace("{} start", name)
    val timed = measureTimedValue(f)
    logger.trace("{} end duration={}", name, timed.duration)
    return timed.value
}
