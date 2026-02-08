package com.moniepoint.storage.system.engine.wal

import java.io.EOFException
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

class WriteAheadLog(private val file: File) {
    private val randomAccessFile = RandomAccessFile(file, "rw")
    private val channel = randomAccessFile.channel

    @Synchronized
    fun append(
        key: String,
        value: ByteArray?,
        isDelete: Boolean = false,
    ) {
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        val valueBytes = if (isDelete) byteArrayOf() else (value ?: byteArrayOf())
        val valueSize = if (isDelete) -1 else valueBytes.size

        val entryPayloadSize = 4 + keyBytes.size + 4 + (if (isDelete) 0 else valueBytes.size)
        val totalSize = 4 + entryPayloadSize

        val buffer = ByteBuffer.allocate(totalSize)
        buffer.putInt(totalSize)
        buffer.putInt(keyBytes.size)
        buffer.put(keyBytes)
        buffer.putInt(valueSize)
        if (!isDelete) {
            buffer.put(valueBytes)
        }

        buffer.flip()
        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
        channel.force(true)
    }

    fun readAllEntries(): List<Pair<String, ByteArray?>> {
        val entries = mutableListOf<Pair<String, ByteArray?>>()
        if (!file.exists() || file.length() == 0L) return entries

        RandomAccessFile(file, "r").use { randomAccessFile ->
            val fileLength = randomAccessFile.length()
            while (randomAccessFile.filePointer < fileLength) {
                try {
                    // 1. Read the header (Total size of this record)
                    val recordSize = randomAccessFile.readInt()

                    // 2. Read Key Size
                    val keySize = randomAccessFile.readInt()
                    if (keySize < 0 || keySize > 1024 * 1024) throw IllegalStateException("Corrupt key size: $keySize")

                    // 3. Read Key (Use readFully!)
                    val keyBytes = ByteArray(keySize)
                    randomAccessFile.readFully(keyBytes)
                    val key = String(keyBytes, Charsets.UTF_8)

                    // 4. Read Value Size
                    val valueSize = randomAccessFile.readInt()

                    // 5. Read Value
                    val value =
                        if (valueSize == -1) {
                            null
                        } else {
                            val valueBytes = ByteArray(valueSize)
                            randomAccessFile.readFully(valueBytes)
                            valueBytes
                        }
                    entries.add(key to value)
                } catch (e: EOFException) {
                    break
                } catch (e: Exception) {
                    println("WAL Restore Error at offset ${randomAccessFile.filePointer}: ${e.message}")
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
