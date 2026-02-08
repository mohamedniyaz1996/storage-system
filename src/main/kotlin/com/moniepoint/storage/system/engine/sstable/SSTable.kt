package com.moniepoint.storage.system.engine.sstable

import java.io.File
import java.nio.ByteBuffer
import java.io.RandomAccessFile

class SSTable(val file: File) {

    private val sparseIndex = mutableMapOf<String, Long>()

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
        var ofset = sparseIndex[floorKey]!!

        RandomAccessFile(file, "r").use { randomAccessFile ->
            randomAccessFile.seek(ofset)
            while (randomAccessFile.filePointer < randomAccessFile.length()) {
                val keySize =randomAccessFile.readInt()
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
}