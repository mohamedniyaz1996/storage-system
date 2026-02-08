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

    fun clear() {
        channel.truncate(0)
    }

    fun close() = randomAccessFile.close()
}