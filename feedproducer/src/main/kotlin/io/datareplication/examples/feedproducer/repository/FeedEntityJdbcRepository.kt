package io.datareplication.examples.feedproducer.repository

import io.datareplication.model.Body
import io.datareplication.model.ContentType
import io.datareplication.model.Entity
import io.datareplication.model.PageId
import io.datareplication.model.Timestamp
import io.datareplication.model.feed.ContentId
import io.datareplication.model.feed.FeedEntityHeader
import io.datareplication.model.feed.OperationType
import io.datareplication.producer.feed.FeedEntityRepository
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

class FeedEntityJdbcRepository(
    private val jdbc: NamedParameterJdbcTemplate,
    private val coro: CoroutineScope
) : FeedEntityRepository {
    private val logger = LoggerFactory.getLogger(this::class.java)

    // TODO extraHeaders
    suspend fun init() = withContext(Dispatchers.IO) {
        jdbc.update(
            """--
CREATE TABLE IF NOT EXISTS feed_entities
(
    content_id        VARCHAR NOT NULL PRIMARY KEY,
    operation_type    VARCHAR NOT NULL,
    ts                BIGINT  NOT NULL,
    ts_nanos          INT     NOT NULL,
    original_ts       BIGINT,
    original_ts_nanos INT,
    page_id           VARCHAR,
    content_type      VARCHAR NOT NULL,
    body              BYTEA   NOT NULL
)""",
            EmptySqlParameterSource()
        )
        jdbc.update(
            """CREATE INDEX IF NOT EXISTS feed_entities_page_index ON feed_entities (page_id)""",
            EmptySqlParameterSource()
        )
    }

    override fun append(entity: Entity<FeedEntityHeader>): CompletionStage<Void> = coro.future(Dispatchers.IO) {
        val params = mapOf(
            "content_id" to entity.header().contentId().value(),
            "operation_type" to entity.header().operationType().toString(),
            "ts" to entity.header().lastModified().value().epochSecond,
            "ts_nanos" to entity.header().lastModified().value().nano,
            "original_ts" to null,
            "original_ts_nanos" to null,
            "page_id" to null,
            "content_type" to entity.body().contentType().value(),
            "body" to entity.body().toBytes()
        )
        timed(logger, "append") {
            jdbc.update(
                """--
INSERT INTO feed_entities (content_id, operation_type, ts, ts_nanos, original_ts, original_ts_nanos, page_id,
                           content_type, body)
VALUES (:content_id, :operation_type, :ts, :ts_nanos, :original_ts, :original_ts_nanos, :page_id, :content_type, :body)""",
                params
            )
            null
        }
    }

    override fun get(pageId: PageId): CompletionStage<MutableList<Entity<FeedEntityHeader>>> =
        coro.future(Dispatchers.IO) {
            val params = mapOf("page_id" to pageId.value())
            timed(logger, "get") {
                jdbc.query(
                    """--
SELECT content_id, operation_type, ts, ts_nanos, content_type, body
FROM feed_entities
WHERE page_id = :page_id
ORDER BY ts, ts_nanos, content_id""",
                    params,
                    ::getEntity
                )
            }
        }

    override fun getUnassigned(limit: Int): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> =
        coro.future(Dispatchers.IO) {
            val params = mapOf("limit" to limit)
            timed(logger, "getUnassigned") {
                jdbc.query(
                    """--
SELECT content_id, ts, ts_nanos, original_ts, original_ts_nanos, length(body) AS content_length, page_id
FROM feed_entities
WHERE page_id IS NULL
ORDER BY ts, ts_nanos, content_id
LIMIT :limit""",
                    params,
                    ::getPageAssignment
                )
            }
        }

    override fun getPageAssignments(pageId: PageId): CompletionStage<MutableList<FeedEntityRepository.PageAssignment>> =
        coro.future(Dispatchers.IO) {
            val params = mapOf("page_id" to pageId.value())
            timed(logger, "getPageAssignments") {
                jdbc.query(
                    """--
SELECT content_id, ts, ts_nanos, original_ts, original_ts_nanos, length(body) AS content_length, page_id
FROM feed_entities
WHERE page_id = :page_id
ORDER BY ts, ts_nanos, content_id""",
                    params,
                    ::getPageAssignment
                )
            }
        }

    override fun savePageAssignments(assignments: List<FeedEntityRepository.PageAssignment>): CompletionStage<Void> =
        coro.future(Dispatchers.IO) {
            val params = assignments.map { assignment ->
                mapOf(
                    "content_id" to assignment.contentId().value(),
                    "ts" to assignment.lastModified().value().epochSecond,
                    "ts_nanos" to assignment.lastModified().value().nano,
                    "original_ts" to assignment.originalLastModified().getOrNull()?.value()?.epochSecond,
                    "original_ts_nanos" to assignment.originalLastModified().getOrNull()?.value()?.nano,
                    // not updating content_length is ok
                    "page_id" to assignment.pageId().getOrNull()?.value()
                )
            }
            timed(logger, "savePageAssignments") {
                jdbc.batchUpdate(
                    """--
UPDATE feed_entities
SET ts                = :ts,
    ts_nanos          = :ts_nanos,
    original_ts       = :original_ts,
    original_ts_nanos = :original_ts_nanos,
    page_id           = :page_id
WHERE content_id = :content_id""",
                    params.toTypedArray()
                )
                null
            }
        }

    private fun getEntity(rs: ResultSet, idx: Int): Entity<FeedEntityHeader> {
        val contentId = ContentId.of(rs.getString("content_id")!!)
        val operationType = OperationType.valueOf(rs.getString("operation_type")!!)
        val ts = rs.getLong("ts")
        val tsNanos = rs.getLong("ts_nanos")
        val lastModified = Timestamp.of(Instant.ofEpochSecond(ts, tsNanos))
        val contentType = ContentType.of(rs.getString("content_type")!!)
        val bodyBytes = rs.getBytes("body")
        return Entity(
            FeedEntityHeader(
                lastModified,
                operationType,
                contentId
            ),
            Body.fromBytesUnsafe(bodyBytes, contentType)
        )
    }

    private fun getPageAssignment(rs: ResultSet, idx: Int): FeedEntityRepository.PageAssignment {
        val contentId = ContentId.of(rs.getString("content_id")!!)
        val ts = rs.getLong("ts")
        val tsNanos = rs.getLong("ts_nanos")
        val originalTs = rs.getLong("original_ts").takeIf { it > 0 }
        val originalTsNanos = rs.getLong("original_ts_nanos")
        val lastModified = Timestamp.of(Instant.ofEpochSecond(ts, tsNanos))
        val originalLastModified =
            originalTs?.let { Instant.ofEpochSecond(it, originalTsNanos) }?.let(Timestamp::of)
        val contentLength = rs.getLong("content_length")
        val pageId = rs.getString("page_id")?.let(PageId::of)
        return FeedEntityRepository.PageAssignment(
            contentId,
            lastModified,
            Optional.ofNullable(originalLastModified),
            contentLength,
            Optional.ofNullable(pageId)
        )
    }
}