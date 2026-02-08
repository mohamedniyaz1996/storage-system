package com.moniepoint.storage.system.engine.wal

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

class WriteAheadLog(private val file: File) {
    private val randomAccessFile = RandomAccessFile(file, "rw")
    private val channel = randomAccessFile.channel

    @Synchronized
    fun append(key: String, value: ByteArray?, isDelete: Boolean = false) {
        val keyBytes = key.toByteArray()
        val valueBytes = value ?: byteArrayOf()
        val valueSize = if(isDelete) -1 else valueBytes.size

        val totalEntrySize = 4 + 4 + keyBytes.size + 4 + valueBytes.size
        val buffer = ByteBuffer.allocate(totalEntrySize)

        buffer.putInt(totalEntrySize)
        buffer.putInt(keyBytes.size)
        buffer.put(keyBytes)
        buffer.putInt(valueSize)
        if (!isDelete) buffer.put(valueBytes)

        buffer.flip()
        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }

        channel.force(true)
    }

    fun readAllEntries(): List<Pair<String, ByteArray?>> {
        val entries = mutableListOf<Pair<String, ByteArray?>>()
        if (file.length() == 0L) return entries

        RandomAccessFile(file, "r").use { randomAccessFile ->
            while (randomAccessFile.filePointer < randomAccessFile.length()) {
                try {
                    val totalEntrySize = randomAccessFile.readInt()
                    val keySize = randomAccessFile.readInt()
                    val keyBytes = ByteArray(keySize)
                    randomAccessFile.read(keyBytes)
                    val key = String(keyBytes)

                    val valueSize = randomAccessFile.readInt()
                    val value = if (valueSize == -1) {
                        null
                    } else {
                        val valueBytes = ByteArray(valueSize)
                        randomAccessFile.read(valueBytes)
                        valueBytes
                    }
                    entries.add(key to value)
                } catch (e: Exception) {
                    println("Exception : ${e.message}")
                    break
                }
            }
        }
        return entries
    }

    fun clear() {
        channel.truncate(0)
    }

    fun close() = randomAccessFile.close()
}