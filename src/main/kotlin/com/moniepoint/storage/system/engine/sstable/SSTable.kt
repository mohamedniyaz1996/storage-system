package com.moniepoint.storage.system.engine.sstable

import com.moniepoint.storage.system.models.SearchResult
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

class SSTable(val file: File) {
    private val sparseIndex = mutableMapOf<String, Long>()

    init {
        if (file.exists() && file.length() > 0) {
            rebuildIndex()
        }
    }

    private fun rebuildIndex() {
        RandomAccessFile(file, "r").use { randomAccessFile ->
            var offset = 0L
            var count = 0
            while (offset < randomAccessFile.length()) {
                if (count % 100 == 0) {
                    randomAccessFile.seek(offset)
                    val keySize = randomAccessFile.readInt()
                    val keyBytes = ByteArray(keySize)
                    randomAccessFile.read(keyBytes)
                    sparseIndex[String(keyBytes)] = offset
                }

                randomAccessFile.seek(offset)
                val keySize = randomAccessFile.readInt()
                randomAccessFile.skipBytes(keySize)
                val valueSize = randomAccessFile.readInt()
                if (valueSize > 0) randomAccessFile.skipBytes(valueSize)

                offset = randomAccessFile.filePointer
                count++
            }
        }
    }

    fun write(entries: Iterable<Map.Entry<String, ByteArray?>>) {
        val randomAccessFile = RandomAccessFile(file, "rw")
        val channel = randomAccessFile.channel
        var currentOffset = 0L

        entries.forEachIndexed { index, entry ->
            val keyBytes = entry.key.toByteArray()
            val valueBytes = entry.value ?: byteArrayOf()
            val valueSize = if (entry.value == null) -1 else valueBytes.size

            if (index % 100 == 0) {
                sparseIndex[entry.key] = currentOffset
            }

            val buffer = ByteBuffer.allocate(4 + keyBytes.size + 4 + valueBytes.size)
            buffer.putInt(keyBytes.size)
            buffer.put(keyBytes)
            buffer.putInt(valueSize)
            if (valueSize != -1) buffer.put(valueBytes)

            buffer.flip()
            while (buffer.hasRemaining()) {
                currentOffset = currentOffset + channel.write(buffer)
            }
        }

        channel.force(true)
        randomAccessFile.close()
    }

    fun get(key: String): ByteArray? {
        val indexedKeys = sparseIndex.keys.sorted()
        val floorKey = indexedKeys.lastOrNull { it <= key } ?: return null
        val offset = sparseIndex[floorKey]!!

        RandomAccessFile(file, "r").use { randomAccessFile ->
            randomAccessFile.seek(offset)
            while (randomAccessFile.filePointer < randomAccessFile.length()) {
                val keySize = randomAccessFile.readInt()
                val keyBytes = ByteArray(keySize)
                randomAccessFile.read(keyBytes)
                val currentKey = String(keyBytes)
                val valueSize = randomAccessFile.readInt()

                if (currentKey == key) {
                    if (valueSize == -1) return null
                    val valueBytes = ByteArray(valueSize)
                    randomAccessFile.read(valueBytes)
                    return valueBytes
                }

                if (currentKey > key) return null

                if (valueSize > 0) randomAccessFile.skipBytes(valueSize)
            }
        }
        return null
    }

    fun getRange(
        startKey: String,
        endKey: String,
    ): List<Pair<String, ByteArray?>> {
        val results = mutableListOf<Pair<String, ByteArray?>>()
        val floorKey = sparseIndex.keys.sorted().lastOrNull { it <= startKey } ?: sparseIndex.keys.minOrNull() ?: return emptyList()
        val offset = sparseIndex[floorKey]!!

        RandomAccessFile(file, "r").use { raf ->
            raf.seek(offset)
            while (raf.filePointer < raf.length()) {
                val kSize = raf.readInt()
                val kBytes = ByteArray(kSize)
                raf.read(kBytes)
                val currentKey = String(kBytes)
                val vSize = raf.readInt()

                if (currentKey > endKey) break

                val vBytes =
                    if (vSize == -1) {
                        null
                    } else {
                        val b = ByteArray(vSize)
                        raf.read(b)
                        b
                    }

                if (currentKey >= startKey) {
                    results.add(currentKey to vBytes)
                }
            }
        }
        return results
    }

    fun getWithTombstone(key: String): SearchResult {
        // 1. Find the best starting point using the Sparse Index
        val indexedKeys = sparseIndex.keys.sorted()
        val floorKey = indexedKeys.lastOrNull { it <= key } ?: return SearchResult(false, null)
        val offset = sparseIndex[floorKey]!!

        RandomAccessFile(file, "r").use { randomAccessFile ->
            randomAccessFile.seek(offset)

            while (randomAccessFile.filePointer < randomAccessFile.length()) {
                val keySize = randomAccessFile.readInt()
                val keyBytes = ByteArray(keySize)
                randomAccessFile.read(keyBytes)
                val currentKey = String(keyBytes)

                val valueSize = randomAccessFile.readInt()

                if (currentKey == key) {
                    return if (valueSize == -1) {
                        SearchResult(true, null)
                    } else {
                        val valueBytes = ByteArray(valueSize)
                        randomAccessFile.read(valueBytes)
                        SearchResult(true, valueBytes)
                    }
                }

                if (currentKey > key) {
                    return SearchResult(false, null)
                }

                if (valueSize > 0) {
                    randomAccessFile.skipBytes(valueSize)
                }
            }
        }
        return SearchResult(false, null)
    }
}
