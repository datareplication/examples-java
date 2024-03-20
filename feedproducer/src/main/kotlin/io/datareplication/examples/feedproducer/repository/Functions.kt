package io.datareplication.examples.feedproducer.repository

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToStream
import kotlinx.serialization.serializer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.exists
import kotlin.io.path.moveTo
import kotlin.io.path.name

@OptIn(ExperimentalSerializationApi::class)
internal fun <T> loadIfExist(path: Path, de: DeserializationStrategy<T>): T? {
    return if (path.exists()) {
        Files.newInputStream(path, StandardOpenOption.READ).use { ins ->
            Json.decodeFromStream(de, ins)
        }
    } else {
        null
    }
}

internal inline fun <reified T> loadIfExist(path: Path): T? {
    return loadIfExist(path, Json.serializersModule.serializer())
}

@OptIn(ExperimentalSerializationApi::class)
internal fun <T> replaceAtomic(path: Path, value: T, ser: SerializationStrategy<T>) {
    val tempName = ".${path.name}.temp"
    val tempFile = path.resolveSibling(tempName)
    Files.newOutputStream(
        tempFile,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
    ).use { out ->
        Json.encodeToStream(ser, value, out)
    }
    tempFile.moveTo(path, overwrite = true)
}

internal inline fun <reified T> replaceAtomic(path: Path, value: T) {
    replaceAtomic(path, value, Json.serializersModule.serializer())
}