package com.moniepoint.storage.system.engine.memtable

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

class MemTable(private val maxSizeBytes: Long) {
    private val data = ConcurrentSkipListMap<String, ByteArray?>()
    private val sizeInBytes = AtomicLong(0)

    fun put(
        key: String,
        value: ByteArray?,
    ) {
        val entrySize = key.length + (value?.size ?: 0)
        data[key] = value
        sizeInBytes.addAndGet(entrySize.toLong())
    }

    fun get(key: String): ByteArray? = data[key]

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

    fun clear() {
        data.clear()
        sizeInBytes.set(0)
    }
}
