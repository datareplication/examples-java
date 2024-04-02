package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.PageId
import io.datareplication.producer.feed.FeedProducerJournalRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.namedparam.EmptySqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.ResultSet
import java.util.Optional
import java.util.concurrent.CompletionStage
import kotlin.jvm.optionals.getOrNull

class FeedProducerJournalJdbcRepository(
    private val jdbc: NamedParameterJdbcTemplate,
    private val coro: CoroutineScope
) : FeedProducerJournalRepository {
    private val logger = LoggerFactory.getLogger(this::class.java)

    suspend fun init() = withContext(Dispatchers.IO) {
        jdbc.update(
            """--
CREATE TABLE IF NOT EXISTS journal
(
    new_pages            TEXT NOT NULL,
    new_latest_page      TEXT NOT NULL,
    previous_latest_page TEXT
); """,
            EmptySqlParameterSource()
        )
    }

    override fun save(state: FeedProducerJournalRepository.JournalState): CompletionStage<Void> =
        coro.future(Dispatchers.IO) {
            val params = mapOf(
                "new_pages" to Json.encodeToString(state.newPages().map { it.value() }),
                "new_latest_page" to state.newLatestPage().value(),
                "previous_latest_page" to state.previousLatestPage().getOrNull()?.value()
            )
            timed(logger, "save") {
                jdbc.update(
                    """--
INSERT INTO journal (new_pages, new_latest_page, previous_latest_page)
VALUES (:new_pages, :new_latest_page, :previous_latest_page)""",
                    params
                )
                null
            }
        }

    override fun get(): CompletionStage<Optional<FeedProducerJournalRepository.JournalState>> =
        coro.future(Dispatchers.IO) {
            timed(logger, "get") {
                jdbc.query(
                    """SELECT new_pages, new_latest_page, previous_latest_page FROM journal LIMIT 1""",
                    EmptySqlParameterSource(),
                    ::getJournalState
                )
                    .singleOrNull()
                    .let { Optional.ofNullable(it) }
            }
        }

    override fun delete(): CompletionStage<Void> = coro.future(Dispatchers.IO) {
        timed(logger, "delete") {
            jdbc.update(
                """DELETE FROM journal""",
                EmptySqlParameterSource()
            )
            null
        }
    }

    private fun getJournalState(rs: ResultSet, idx: Int): FeedProducerJournalRepository.JournalState {
        val newPages = Json.decodeFromString<List<String>>(rs.getString("new_pages")!!).map(PageId::of)
        val newLatestPage = PageId.of(rs.getString("new_latest_page")!!)
        val previousLatestPage = rs.getString("previous_latest_page")?.let(PageId::of)
        return FeedProducerJournalRepository.JournalState(
            newPages,
            newLatestPage,
            Optional.ofNullable(previousLatestPage)
        )
    }
}