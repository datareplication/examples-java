package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.Body
import io.datareplication.model.ContentType
import io.datareplication.model.Entity
import io.datareplication.model.HttpHeader
import io.datareplication.model.HttpHeaders
import io.datareplication.model.PageId
import io.datareplication.model.Timestamp
import io.datareplication.model.feed.ContentId
import io.datareplication.model.feed.FeedEntityHeader
import io.datareplication.model.feed.OperationType
import io.datareplication.producer.feed.FeedEntityRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import kotlinx.serialization.Serializable
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import kotlin.jvm.optionals.getOrNull

@Serializable
private data class PageIndex(val items: List<String>)

@Serializable
private data class StoredEntity(
    val operationType: String,
    val contentId: String,
    val extraHeaders: Map<String, List<String>>,
    val lastModified: String,
    val originalLastModified: String?,
    val pageId: String?,
    val contentLength: Long,
    val contentType: String,
    val body: String
) {
    fun pageAssignment(): Result<FeedEntityRepository.PageAssignment> = Result.runCatching {
        FeedEntityRepository.PageAssignment(
            ContentId.of(contentId),
            Instant.parse(lastModified).let(Timestamp::of),
            originalLastModified?.let(Instant::parse)?.let(Timestamp::of).let { Optional.ofNullable(it) },
            contentLength,
            pageId?.let(PageId::of).let { Optional.ofNullable(it) }
        )
    }

    fun entity(): Result<Entity<FeedEntityHeader>> = Result.runCatching {
        val extraHeaders = extraHeaders
            .map { HttpHeader.of(it.key, it.value) }
            .let { HttpHeaders.of(it) }
        val bodyBytes = body.toByteArray(StandardCharsets.ISO_8859_1)
        Entity(
            FeedEntityHeader(
                Instant.parse(lastModified).let(Timestamp::of),
                OperationType.valueOf(operationType),
                ContentId.of(contentId),
                extraHeaders
            ),
            Body.fromBytesUnsafe(bodyBytes, ContentType.of(contentType))
        )
    }
}

private fun Entity<FeedEntityHeader>.toStored(): StoredEntity {
    val bodyAsString = String(body().toBytes(), StandardCharsets.ISO_8859_1)
    val extraHeaders = header()
        .extraHeaders()
        .associate { it.displayName() to it.values() }
    return StoredEntity(
        operationType = header().operationType().toString(),
        contentId = header().contentId().value(),
        extraHeaders = extraHeaders,
        lastModified = header().lastModified().value().toString(),
        originalLastModified = null,
        pageId = null,
        contentLength = body().contentLength(),
        contentType = body().contentType().value(),
        body = bodyAsString
    )
}

private fun Entity<FeedEntityHeader>.fileName(): String {
    val timestamp = header().lastModified().value().toString()
    return "${timestamp}_${header().contentId().value()}"
}

private fun PageId?.indexFileName(): String = if (this != null) {
    "index-page-${value()}"
} else {
    "index-unassigned"
}

class FeedEntityFileRepository(
    private val path: Path,
    private val coro: CoroutineScope
) : FeedEntityRepository {
    init {
        Files.createDirectories(path)
    }

    override fun append(entity: Entity<FeedEntityHeader>): CompletionStage<Void> = coro.future {
        val p = path.resolve(entity.fileName())
        val stored = entity.toStored()
        replaceAtomic(p, stored)
        null
    }

    override fun get(pageId: PageId): CompletionStage<MutableList<Entity<FeedEntityHeader>>> = coro.future {
        getFromIndex(pageId)
            .map { it.entity().getOrThrow() }
            .toMutableList()
    }

    override fun getUnassigned(limit: Int): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> =
        coro.future {
            getFromIndex(null)
                .take(limit)
                .map { it.pageAssignment().getOrThrow() }
                .toMutableList()
        }

    override fun getPageAssignments(pageId: PageId): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> =
        coro.future {
            getFromIndex(pageId)
                .map { it.pageAssignment().getOrThrow() }
                .toMutableList()
        }

    private fun getFromIndex(pageId: PageId?): List<StoredEntity> {
        val p = path.resolve(pageId.indexFileName())
        return loadIfExist<PageIndex>(p)
            ?.items
            .orEmpty()
            .sorted()
            .map { name ->
                val entityPath = path.resolve(name)
                loadIfExist<StoredEntity>(entityPath) ?: throw IOException("file missing that we just listed")
            }
            .filter { it.pageId == pageId?.value() }
    }

    // TODO: an assumption I want to make here: it is ok to save any change other than the page ID change (i.e. timestamps)
    //  and then update the page ID
    //  this allows updating the metadata first and then changing the page by atomically renaming the file (/moving it
    //  into a different directory)
    //  ideally the API would be changed to make this more explicitly ok
    override fun savePageAssignments(assignments: List<FeedEntityRepository.PageAssignment>): CompletionStage<Void> =
        coro.future {
            assignments.groupBy { it.pageId().getOrNull() }
        }
}