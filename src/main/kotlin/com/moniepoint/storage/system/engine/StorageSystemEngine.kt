package com.moniepoint.storage.system.engine

import com.moniepoint.storage.system.contract.StorageEngineInterface
import com.moniepoint.storage.system.engine.memtable.MemTable
import com.moniepoint.storage.system.engine.sstable.SSTable
import com.moniepoint.storage.system.engine.wal.WriteAheadLog
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The core Log-Structured Merge-Tree (LSM-Tree) storage engine.
 * * This class orchestrates the lifecycle of data from volatile memory to persistent
 * storage, coordinating the MemTable, Write-Ahead Log (WAL), and SSTables to
 * ensure ACID compliance and high-performance throughput.
 *
 * ### Data Flow
 * 1. **Write Path:** Data is first appended to the [WriteAheadLog] for durability,
 * then stored in the [MemTable]. When the MemTable exceeds its size threshold,
 * it is "flushed"—serialized into a new [SSTable] file and the WAL is cleared.
 * 2. **Read Path:** Searches follow a strict hierarchy: [MemTable] (most recent)
 * -> [ssTables] (ordered by newest to oldest). This ensures the most up-to-date
 * version of a key is always returned.
 * *
 *
 * ### Key Responsibilities
 * * **Crash Recovery:** On startup, replays the [WAL] to reconstruct any data
 * that wasn't flushed to an SSTable before a shutdown.
 * * **Concurrency Management:** Uses thread-safe collections for SSTable tracking
 * and synchronization on write operations to maintain consistency.
 * * **Compaction Readiness:** Manages the sequence numbering of immutable
 * SSTable files (`.db`) stored in the root directory.
 * * **Deletion Handling:** Implements "tombstones" to mark data as deleted
 * across the distributed storage layers.
 *
 */
@Singleton
class StorageSystemEngine(
    @property:Property(name = "storage.root-dir") private val rootDirPath: String = "data",
    private val memTable: MemTable,
) : StorageEngineInterface {
    private val rootDir = File(rootDirPath).apply { if (!exists()) mkdirs() }
    private val wal = WriteAheadLog(File(rootDir, "current.wal"))

    private val ssTables = CopyOnWriteArrayList<SSTable>()

    private var nextSequence = 0L

    init {
        val existingFiles =
            rootDir.listFiles { _, name -> name.endsWith(".db") }
                ?.sortedByDescending { it.name } // Newest sequence first

        existingFiles?.forEach { file ->
            ssTables.add(SSTable(file))
            val seq = file.name.substringBefore(".db").toLongOrNull() ?: 0L
            if (seq >= nextSequence) {
                nextSequence = seq + 1
            }
        }

        recoverFromWal()
    }

    private fun recoverFromWal() {
        val walFile = File(rootDir, "current.wal")
        if (!walFile.exists()) return

        val entries = wal.readAllEntries()
        entries.forEach { (key, value) ->
            memTable.put(key, value)
        }
    }

    private fun flushMemTable() {
        val fileName = String.format("%010d.db", nextSequence++)
        val tempFile = File(rootDir, "$fileName.tmp")
        val finalFile = File(rootDir, fileName)

        val newTable = SSTable(tempFile)
        newTable.write(memTable.retrieveSortedEntries())

        tempFile.renameTo(finalFile)

        ssTables.add(0, SSTable(finalFile))

        memTable.clear()
        wal.clear()
    }

    fun resetInternalState() {
        memTable.clear()
        ssTables.clear()
        wal.clear()
        nextSequence = 0L
        rootDir.listFiles()?.forEach { it.delete() }
    }

    @Synchronized
    override fun put(
        key: String,
        value: ByteArray,
    ) {
        wal.append(key, value)
        memTable.put(key, value)

        if (memTable.isOverFull()) {
            flushMemTable()
        }
    }

    override fun batchPut(items: Map<String, ByteArray>) {
        items.forEach { (key, value) -> put(key, value) }
    }

    override fun read(key: String): ByteArray? {
        if (memTable.containsKey(key)) {
            return memTable.get(key)
        }

        for (table in ssTables) {
            val result = table.getWithTombstone(key)
            if (result.found) {
                return result.value
            }
        }

        return null
    }

    override fun readKeyRange(
        startKey: String,
        endKey: String,
    ): List<Pair<String, ByteArray?>> {
        val resultTracker = mutableMapOf<String, ByteArray?>()

        ssTables.reversed().forEach { table ->
            table.getRange(startKey, endKey).forEach { (k, v) ->
                resultTracker[k] = v
            }
        }

        memTable.getRange(startKey, endKey).forEach { (k, v) ->
            resultTracker[k] = v
        }

        return resultTracker.entries
            .filter { it.value != null }
            .map { it.key to it.value }
            .sortedBy { it.first }
    }

    override fun delete(key: String) {
        wal.append(key, null, isDelete = true)
        memTable.put(key, null)

        if (memTable.isOverFull()) {
            flushMemTable()
        }
    }
}
