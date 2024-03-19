package io.datareplication.examples.feedproducer.repository

import io.datareplication.producer.feed.FeedProducerJournalRepository
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.CompletionStage

class FeedProducerJournalFileRepository(private val path: Path) : FeedProducerJournalRepository {
    override fun save(state: FeedProducerJournalRepository.JournalState): CompletionStage<Void> {
        TODO("Not yet implemented")
    }

    override fun get(): CompletionStage<Optional<FeedProducerJournalRepository.JournalState>> {
        TODO("Not yet implemented")
    }

    override fun delete(): CompletionStage<Void> {
        TODO("Not yet implemented")
    }
}