package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.PageId
import io.datareplication.producer.feed.FeedProducerJournalRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.CompletionStage
import kotlin.io.path.deleteIfExists
import kotlin.jvm.optionals.getOrNull

private data class StoredJournal(
    val newPages: List<String>,
    val newLatestPage: String,
    val previousLatestPage: String?
) {
    fun journalState(): FeedProducerJournalRepository.JournalState {
        return FeedProducerJournalRepository.JournalState(
            newPages.map(PageId::of),
            PageId.of(newLatestPage),
            previousLatestPage?.let(PageId::of).let { Optional.ofNullable(it) }
        )
    }
}

private fun FeedProducerJournalRepository.JournalState.toStored(): StoredJournal {
    return StoredJournal(
        newPages = newPages().map { it.value() },
        newLatestPage = newLatestPage().value(),
        previousLatestPage = previousLatestPage().getOrNull()?.value()
    )
}

class FeedProducerJournalFileRepository(
    private val path: Path,
    private val coro: CoroutineScope
) : FeedProducerJournalRepository {
    override fun save(state: FeedProducerJournalRepository.JournalState): CompletionStage<Void> = coro.future {
        val stored = state.toStored()
        replaceAtomic(path, stored)
        null
    }

    override fun get(): CompletionStage<Optional<FeedProducerJournalRepository.JournalState>> = coro.future {
        loadIfExist<StoredJournal>(path)?.journalState().let { Optional.ofNullable(it) }
    }

    override fun delete(): CompletionStage<Void> = coro.future {
        path.deleteIfExists()
        null
    }
}
