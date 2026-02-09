package com.moniepoint.storage.system.engine.memtable

import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

/**
 * An in-memory, thread-safe data structure that buffers writes before they are flushed to SSTables.
 *
 * The MemTable serves as the first point of entry for all write operations. It provides
 * high-speed lookups and keeps data sorted to facilitate efficient range queries and
 * eventual conversion into an [SSTable].
 *
 * ### Internal Architecture
 * * **ConcurrentSkipListMap:** Uses a skip list to provide log-time search and insertion
 * while maintaining natural key order. This structure is lock-free and thread-safe.
 * * **Tombstone Support:** Uses a specific marker ([TOMBSTONE]) to represent deleted keys,
 * ensuring that deletions are correctly propagated during the flush process.
 * * **Size Tracking:** Maintains an [AtomicLong] counter to track the approximate memory
 * footprint, triggering a flush when the [maxSizeBytes] threshold is exceeded.
 *
 *
 *
 * ### Key Features
 * * **Ordered Retrieval:** Data is always sorted lexicographically by key.
 * * **Concurrency:** Optimized for high-throughput concurrent reads and writes.
 * * **Capacity Management:** Provides [isOverFull] to inform the engine when the
 * table should be frozen and persisted to disk.
 *
 */
@Singleton
class MemTable(
    @property:Property(name = "storage.mem-table-max-bytes")
    private val maxSizeBytes: Long = 67108864,
) {
    private val data = ConcurrentSkipListMap<String, ByteArray?>()
    private val sizeInBytes = AtomicLong(0)

    fun put(
        key: String,
        value: ByteArray?,
    ) {
        val actualValue = value ?: TOMBSTONE // Use marker if null

        val entrySize = key.length + actualValue.size
        data[key] = actualValue
        sizeInBytes.addAndGet(entrySize.toLong())
    }

    fun get(key: String): ByteArray? {
        val result = data[key]
        return if (result === TOMBSTONE) null else result
    }

    fun getRange(
        startKey: String,
        endKey: String,
    ): List<Pair<String, ByteArray?>> {
        return data.subMap(startKey, true, endKey, true)
            .entries.filter { it.value != null }
            .map { it.key to it.value!! }
    }

    fun isOverFull(): Boolean = sizeInBytes.get() >= maxSizeBytes

    fun retrieveSortedEntries() = data.entries

    fun containsKey(key: String) = data.containsKey(key)

    fun clear() {
        data.clear()
        sizeInBytes.set(0)
    }

    companion object {
        val TOMBSTONE = byteArrayOf()
    }
}
