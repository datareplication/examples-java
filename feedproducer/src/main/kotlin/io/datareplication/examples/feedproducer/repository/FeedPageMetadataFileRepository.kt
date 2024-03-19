package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.PageId
import io.datareplication.producer.feed.FeedPageMetadataRepository
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.CompletionStage

class FeedPageMetadataFileRepository(private val path: Path) : FeedPageMetadataRepository {
    override fun get(pageId: PageId): CompletionStage<Optional<FeedPageMetadataRepository.PageMetadata>> {
        TODO("Not yet implemented")
    }

    override fun getWithoutNextLink(): CompletionStage<MutableList<FeedPageMetadataRepository.PageMetadata>> {
        TODO("Not yet implemented")
    }

    override fun save(pages: List<FeedPageMetadataRepository.PageMetadata>): CompletionStage<Void> {
        TODO("Not yet implemented")
    }

    override fun delete(pages: List<PageId>): CompletionStage<Void> {
        TODO("Not yet implemented")
    }
}