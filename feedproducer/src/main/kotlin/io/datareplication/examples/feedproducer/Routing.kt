package io.datareplication.examples.feedproducer

import io.datareplication.model.Page
import io.datareplication.model.PageId
import io.datareplication.model.feed.FeedEntityHeader
import io.datareplication.model.feed.FeedPageHeader
import io.datareplication.producer.feed.FeedPageProvider
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytesWriter
import io.ktor.server.routing.get
import io.ktor.server.routing.head
import io.ktor.server.routing.routing
import io.ktor.utils.io.jvm.javaio.copyTo
import io.ktor.utils.io.jvm.javaio.toOutputStream
import kotlinx.coroutines.future.await
import kotlin.jvm.optionals.getOrNull

private suspend fun respondGet(call: ApplicationCall, page: Page<FeedPageHeader, FeedEntityHeader>) {
    for (header in page.header().toHttpHeaders()) {
        for (value in header.values()) {
            call.response.header(header.name(), value)
        }
    }
    val body = page.toMultipartBody()
    call.respondBytesWriter(
        contentType = ContentType.parse(body.contentType().value()),
        contentLength = body.contentLength()
    ) { body.newInputStream().copyTo(this.toOutputStream()) }
}

private suspend fun respondHead(call: ApplicationCall, pageHeader: FeedPageProvider.HeaderAndContentType) {
    for (header in pageHeader.header().toHttpHeaders()) {
        for (value in header.values()) {
            call.response.header(header.name(), value)
        }
    }
    call.response.header(HttpHeaders.ContentType, pageHeader.contentType().value())
    call.respond(HttpStatusCode.OK)
}

fun Application.feedProducerRoutes(feedPageProvider: FeedPageProvider) {
    routing {
        get("/feed/{pageId}") {
            val pageId = call.parameters["pageId"]?.let(PageId::of)
            val page = feedPageProvider
                .page(pageId!!)
                .await()
                .getOrNull()
            if (page != null) {
                respondGet(call, page)
            } else {
                call.respond(HttpStatusCode.NotFound)
            }
        }

        head("/feed/{pageId}") {
            val pageId = call.parameters["pageId"]?.let(PageId::of)
            val pageHeader = feedPageProvider
                .pageHeader(pageId!!)
                .await()
                .getOrNull()
            if (pageHeader != null) {
                respondHead(call, pageHeader)
            } else {
                call.respond(HttpStatusCode.NotFound)
            }
        }

        get("/feed/latest") {
            val page = feedPageProvider
                .latestPageId()
                .await()
                .getOrNull()
                ?.let(feedPageProvider::page)
                ?.await()
                ?.getOrNull()
            if (page != null) {
                respondGet(call, page)
            } else {
                call.respond(HttpStatusCode.NoContent)
            }
        }

        head("/feed/latest") {
            val pageHeader = feedPageProvider
                .latestPageId()
                .await()
                .getOrNull()
                ?.let(feedPageProvider::pageHeader)
                ?.await()
                ?.getOrNull()
            if (pageHeader != null) {
                respondHead(call, pageHeader)
            } else {
                call.respond(HttpStatusCode.NoContent)
            }
        }
    }
}