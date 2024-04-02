package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.PageId
import io.datareplication.model.Timestamp
import io.datareplication.producer.feed.FeedPageMetadataRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.serialization.Serializable
import java.nio.file.Path
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import kotlin.io.path.deleteIfExists
import kotlin.jvm.optionals.getOrNull

@Serializable
private data class StoredPage(
    val pageId: String,
    val lastModified: String,
    val prev: String?,
    val next: String?,
    val numberOfBytes: Long,
    val numberOfEntities: Int,
    val generation: Int
) {
    fun toPage(): Result<FeedPageMetadataRepository.PageMetadata> = Result.runCatching {
        FeedPageMetadataRepository.PageMetadata(
            PageId.of(pageId),
            Instant.parse(lastModified).let(Timestamp::of),
            prev?.let(PageId::of).let { Optional.ofNullable(it) },
            next?.let(PageId::of).let { Optional.ofNullable(it) },
            numberOfBytes,
            numberOfEntities,
            generation
        )
    }
}

private fun FeedPageMetadataRepository.PageMetadata.toStored(): StoredPage {
    return StoredPage(
        pageId = pageId().value(),
        lastModified = lastModified().value().toString(),
        prev = prev().getOrNull()?.value(),
        next = next().getOrNull()?.value(),
        numberOfBytes = numberOfBytes(),
        numberOfEntities = numberOfEntities(),
        generation = generation()
    )
}

private fun PageId.fileName(): String = "page-${value()}"

class FeedPageMetadataFileRepository(
    private val path: Path,
    private val coro: CoroutineScope
) : FeedPageMetadataRepository {
    private val emptyNextLinkIndex = path.resolve("empty-next-link-index")

    override fun get(pageId: PageId): CompletionStage<Optional<FeedPageMetadataRepository.PageMetadata>> = coro.future {
        val p = path.resolve(pageId.fileName())
        val stored = loadIfExist<StoredPage>(p)
        stored?.toPage()?.getOrThrow().let { Optional.ofNullable(it) }
    }

    // TODO note: when saving pages, it is ok to first extend a separate "index of pages with no next link", then save
    //  the pages, then shrink the index
    //  then we try to load every page in the index and filter out any that do have a next link
    //  that's fully consistent
    override fun getWithoutNextLink(): CompletionStage<MutableList<FeedPageMetadataRepository.PageMetadata>> =
        coro.future {
            loadIfExist<Index>(emptyNextLinkIndex)
                ?.items
                .orEmpty()
                .mapNotNull { id ->
                    val maybePage = get(PageId.of(id)).await().getOrNull()
                    maybePage?.takeIf { it.next().isEmpty }
                }
                .toMutableList()
        }

    override fun save(pages: List<FeedPageMetadataRepository.PageMetadata>): CompletionStage<Void> = coro.future {
        val initialIndex = loadIfExist<Index>(emptyNextLinkIndex).orEmpty()
        val allIds = pages.map { it.pageId().value() }.toSet()
        val contained = pages.filter { it.next().isEmpty }.map { it.pageId().value() }.toSet()
        val newIndex1 = initialIndex.extend(contained)
        replaceAtomic(emptyNextLinkIndex, newIndex1)
        for (page in pages) {
            val p = path.resolve(page.pageId().fileName())
            val stored = page.toStored()
            replaceAtomic(p, stored)
        }
        val newIndex2 = newIndex1.shrink(contained, allIds)
        replaceAtomic(emptyNextLinkIndex, newIndex2)
        null
    }

    override fun delete(pages: List<PageId>): CompletionStage<Void> = coro.future {
        for (id in pages) {
            val p = path.resolve(id.fileName())
            p.deleteIfExists()
        }
        null
    }
}