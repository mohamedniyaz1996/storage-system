package com.moniepoint.storage.system.engine.sstable

import com.moniepoint.storage.system.engine.bloomfilter.BloomFilter
import com.moniepoint.storage.system.models.SearchResult
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

class SSTable(val file: File) {
    private val sparseIndex = mutableMapOf<String, Long>()
    private val bloomFilter = BloomFilter(size = 100_000, numHashFunctions = 3)

    init {
        if (file.exists() && file.length() > 0) {
            rebuildIndexAndFilter()
        }
    }

    private fun rebuildIndexAndFilter() {
        RandomAccessFile(file, "r").use { randomAccessFile ->
            var offset = 0L
            var count = 0
            while (offset < randomAccessFile.length()) {
                randomAccessFile.seek(offset)
                val keySize = randomAccessFile.readInt()
                val keyBytes = ByteArray(keySize)
                randomAccessFile.readFully(keyBytes)
                val key = String(keyBytes)

                bloomFilter.add(key)

                if (count % 100 == 0) {
                    sparseIndex[key] = offset
                }

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
            bloomFilter.add(entry.key)
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
        if (!bloomFilter.mightContain(key)) {
            return SearchResult(false, null) // skip disk-search entirely
        }

        val indexedKeys = sparseIndex.keys.sorted()
        val floorKey = indexedKeys.lastOrNull { it <= key } ?: return SearchResult(false, null)
        val offset = sparseIndex[floorKey]!!

        RandomAccessFile(file, "r").use { randomAccessFile ->
            randomAccessFile.seek(offset)

            while (randomAccessFile.filePointer < randomAccessFile.length()) {
                val keySize = randomAccessFile.readInt()
                val keyBytes = ByteArray(keySize)
                randomAccessFile.readFully(keyBytes)
                val currentKey = String(keyBytes)
                val valueSize = randomAccessFile.readInt()

                if (currentKey == key) {
                    return if (valueSize == -1) {
                        SearchResult(true, null)
                    } else {
                        SearchResult(true, readValue(randomAccessFile, valueSize))
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

    private fun readValue(
        raf: RandomAccessFile,
        size: Int,
    ): ByteArray {
        val b = ByteArray(size)
        raf.readFully(b)
        return b
    }
}
