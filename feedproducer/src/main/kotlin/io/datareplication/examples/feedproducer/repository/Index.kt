package io.datareplication.examples.feedproducer.repository

import kotlinx.serialization.Serializable

@Serializable
data class Index(val items: Set<String>) {
    fun extend(contained: Set<String>): Index = Index(items.union(contained))

    fun shrink(contained: Set<String>, all: Set<String>): Index {
        return items
            .filter { all.contains(it) && !contained.contains(it) }
            .toSet()
            .let(::Index)
    }

    companion object {
        val EMPTY = Index(emptySet())
    }
}

fun Index?.orEmpty(): Index = this ?: Index.EMPTY
