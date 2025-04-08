package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.PageId
import io.datareplication.producer.feed.FeedPageMetadataRepository
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.namedparam.EmptySqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.ResultSet
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletionStage
import kotlin.jvm.optionals.getOrNull

class FeedPageMetadataJdbcRepository(
    private val jdbc: NamedParameterJdbcTemplate,
    private val coro: CoroutineScope
) : FeedPageMetadataRepository {
    private val logger = LoggerFactory.getLogger(this::class.java)

    suspend fun init() = withContext(Dispatchers.IO) {
        jdbc.update(
            """--
CREATE TABLE IF NOT EXISTS feed_pages
(
    page_id            TEXT   NOT NULL PRIMARY KEY,
    ts                 BIGINT NOT NULL,
    ts_nanos           INT    NOT NULL,
    prev               TEXT,
    next               TEXT,
    number_of_bytes    BIGINT NOT NULL,
    number_of_entities INT    NOT NULL,
    generation         INT    NOT NULL
)""",
            EmptySqlParameterSource()
        )
        jdbc.update(
            """CREATE INDEX IF NOT EXISTS feed_pages_next_index ON feed_pages (next)""",
            EmptySqlParameterSource()
        )
    }

    override fun get(pageId: PageId): CompletionStage<Optional<FeedPageMetadataRepository.PageMetadata>> =
        coro.future(Dispatchers.IO) {
            val params = mapOf("page_id" to pageId.value())
            timed(logger, "get") {
                jdbc.query(
                    """--
SELECT page_id,
       ts,
       ts_nanos,
       prev,
       next,
       number_of_bytes,
       number_of_entities,
       generation
FROM feed_pages
WHERE page_id = :page_id
LIMIT 1""",
                    params,
                    ::getPageMetadata
                )
                    .singleOrNull()
                    .let { Optional.ofNullable(it) }
            }
        }

    override fun getWithoutNextLink(): CompletionStage<MutableList<FeedPageMetadataRepository.PageMetadata>> =
        coro.future(Dispatchers.IO) {
            timed(logger, "getWithoutNextLink") {
                jdbc.query(
                    """--
SELECT page_id,
       ts,
       ts_nanos,
       prev,
       next,
       number_of_bytes,
       number_of_entities,
       generation
FROM feed_pages
WHERE next IS NULL""",
                    EmptySqlParameterSource(),
                    ::getPageMetadata
                )
            }
        }

    override fun save(pages: List<FeedPageMetadataRepository.PageMetadata>): CompletionStage<Void> =
        coro.future(Dispatchers.IO) {
            val params = pages.map { page ->
                mapOf(
                    "page_id" to page.pageId().value(),
                    "ts" to page.lastModified().epochSecond,
                    "ts_nanos" to page.lastModified().nano,
                    "prev" to page.prev().getOrNull()?.value(),
                    "next" to page.next().getOrNull()?.value(),
                    "number_of_bytes" to page.numberOfBytes(),
                    "number_of_entities" to page.numberOfEntities(),
                    "generation" to page.generation()
                )
            }
            timed(logger, "save") {
                jdbc.batchUpdate(
                    """--
INSERT INTO feed_pages (page_id, ts, ts_nanos, prev, next, number_of_bytes, number_of_entities, generation)
VALUES (:page_id, :ts, :ts_nanos, :prev, :next, :number_of_bytes, :number_of_entities, :generation)
ON CONFLICT (page_id) DO UPDATE SET ts                 = excluded.ts,
                                    ts_nanos           = excluded.ts_nanos,
                                    prev               = excluded.prev,
                                    next               = excluded.next,
                                    number_of_bytes    = excluded.number_of_bytes,
                                    number_of_entities = excluded.number_of_entities,
                                    generation         = excluded.generation""",
                    params.toTypedArray()
                )
                null
            }
        }

    override fun delete(pageIds: List<PageId>): CompletionStage<Void> = coro.future(Dispatchers.IO) {
        val params = pageIds.map { pageId -> mapOf("page_id" to pageId.value()) }
        timed(logger, "delete") {
            jdbc.batchUpdate(
                """DELETE FROM feed_pages WHERE page_id = :page_id""",
                params.toTypedArray()
            )
            null
        }
    }

    private fun getPageMetadata(rs: ResultSet, idx: Int): FeedPageMetadataRepository.PageMetadata {
        val pageId = PageId.of(rs.getString("page_id")!!)
        val ts = rs.getLong("ts")
        val tsNanos = rs.getLong("ts_nanos")
        val lastModified = Instant.ofEpochSecond(ts, tsNanos)
        val prev = rs.getString("prev")?.let(PageId::of)
        val next = rs.getString("next")?.let(PageId::of)
        val numberOfBytes = rs.getLong("number_of_bytes")
        val numberOfEntities = rs.getInt("number_of_entities")
        val generation = rs.getInt("generation")
        return FeedPageMetadataRepository.PageMetadata(
            pageId,
            lastModified,
            Optional.ofNullable(prev),
            Optional.ofNullable(next),
            numberOfBytes,
            numberOfEntities,
            generation
        )
    }
}