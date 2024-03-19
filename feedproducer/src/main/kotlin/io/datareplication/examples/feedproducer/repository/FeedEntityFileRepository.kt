package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.Entity
import io.datareplication.model.PageId
import io.datareplication.model.feed.FeedEntityHeader
import io.datareplication.producer.feed.FeedEntityRepository
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

fun Entity<FeedEntityHeader>.fileName(): String {
    val epochSeconds = header().lastModified().value().epochSecond
    val nanos = header().lastModified().value().nano
    return "${epochSeconds}_${nanos}_${header().contentId().value()}"
}

class FeedEntityFileRepository(private val path: Path) : FeedEntityRepository {
    init {
        Files.createDirectories(path)
    }

    override fun append(entity: Entity<FeedEntityHeader>): CompletionStage<Void> {
        return CompletableFuture.supplyAsync {
            val p = path.resolve(entity.fileName())
            Files.write(p, entity.body().toBytes())
            null
        }
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

    // TODO: an assumption I want to make here: it is ok to save any change other than the page ID change (i.e. timestamps)
    //  and then update the page ID
    //  this allows updating the metadata first and then changing the page by atomically renaming the file (/moving it
    //  into a different directory)
    //  ideally the API would be changed to make this more explicitly ok
    override fun savePageAssignments(assignments: List<FeedEntityRepository.PageAssignment>): CompletionStage<Void> {
        TODO("Not yet implemented")
    }
}