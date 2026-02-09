package com.moniepoint.storage.system.engine.wal

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * A simple, persistent Write-Ahead Log (WAL) designed for data durability.
 *
 * This class records every operation to a file on disk before it is applied to
 * the main database. If the system crashes, the log can be replayed to restore
 * the in-memory state.
 *
 * Each log entry is stored in the following binary format:
 * 1. **Total Size** (4 bytes): Length of the entire record.
 * 2. **CRC Checksum** (8 bytes): Protection against data corruption.
 * 3. **Key Size** (4 bytes): Length of the key string.
 * 4. **Key** (Variable): The actual key bytes.
 * 5. **Value Size** (4 bytes): Length of the value (-1 indicates a deletion).
 * 6. **Value** (Variable): The actual data (skipped if deleted).
 *
 */
class WriteAheadLog(private val file: File) {
    private val randomAccessFile = RandomAccessFile(file, "rw")
    private val channel = randomAccessFile.channel
    private val crc = CRC32()

    @Synchronized
    fun append(
        key: String,
        value: ByteArray?,
        isDelete: Boolean = false,
    ) {
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        val valueBytes = value ?: byteArrayOf()
        val valueSize = if (isDelete) -1 else valueBytes.size

        // Layout: [TotalSize:4] [CRC:8] [KeySize:4] [Key] [ValSize:4] [Val]
        val payloadSize = 4 + keyBytes.size + 4 + (if (isDelete) 0 else valueBytes.size)
        val totalSize = 4 + 8 + payloadSize

        val buffer = ByteBuffer.allocate(totalSize)
        buffer.putInt(totalSize)
        buffer.putLong(0L) // Placeholder for CRC
        buffer.putInt(keyBytes.size)
        buffer.put(keyBytes)
        buffer.putInt(valueSize)
        if (!isDelete) buffer.put(valueBytes)

        // Calculate CRC on the data payload (from offset 12 to end)
        crc.reset()
        buffer.position(12)
        crc.update(buffer)
        buffer.putLong(4, crc.value)

        buffer.flip()
        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
        channel.force(true)
    }

    fun readAllEntries(): List<Pair<String, ByteArray?>> {
        val entries = mutableListOf<Pair<String, ByteArray?>>()
        if (!file.exists() || file.length() < 12) return entries

        randomAccessFile.seek(0)
        while (randomAccessFile.filePointer < randomAccessFile.length()) {
            val startOffset = randomAccessFile.filePointer
            try {
                val totalSize = randomAccessFile.readInt()
                val storedCrc = randomAccessFile.readLong()

                // Read the rest of the record to verify CRC
                val payloadSize = totalSize - 12
                val payload = ByteArray(payloadSize)
                randomAccessFile.readFully(payload)

                crc.reset()
                crc.update(payload)
                if (crc.value != storedCrc) {
                    println("Corruption detected at offset $startOffset. Stopping recovery.")
                    break
                }

                val buffer = ByteBuffer.wrap(payload)
                val keySize = buffer.getInt()
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                val valueSize = buffer.getInt()
                val value =
                    if (valueSize == -1) {
                        null
                    } else {
                        val valueBytes = ByteArray(valueSize)
                        buffer.get(valueBytes)
                        valueBytes
                    }
                entries.add(String(keyBytes) to value)
            } catch (e: Exception) {
                break
            }
        }
        return entries
    }

    fun clear() {
        channel.truncate(0)
        channel.force(true)
    }
}
