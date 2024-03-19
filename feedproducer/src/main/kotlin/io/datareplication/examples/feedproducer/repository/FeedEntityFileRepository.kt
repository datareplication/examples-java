package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.Entity
import io.datareplication.model.PageId
import io.datareplication.model.feed.FeedEntityHeader
import io.datareplication.producer.feed.FeedEntityRepository
import java.nio.file.Path
import java.util.concurrent.CompletionStage

class FeedEntityFileRepository(private val path: Path) : FeedEntityRepository {
    override fun append(entity: Entity<FeedEntityHeader>): CompletionStage<Void> {
        TODO("Not yet implemented")
    }

    override fun get(pageId: PageId): CompletionStage<MutableList<Entity<FeedEntityHeader>>> {
        TODO("Not yet implemented")
    }

    override fun getUnassigned(limit: Int): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> {
        TODO("Not yet implemented")
    }

    override fun getPageAssignments(pageId: PageId): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> {
        TODO("Not yet implemented")
    }

    override fun savePageAssignments(assignments: List<FeedEntityRepository.PageAssignment>): CompletionStage<Void> {
        TODO("Not yet implemented")
    }
}