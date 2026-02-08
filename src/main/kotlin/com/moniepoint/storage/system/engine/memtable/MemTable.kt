package com.moniepoint.storage.system.engine.memtable

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

class MemTable(private val maxSizeBytes: Long = 128 * 1024 * 1024) {
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
