package com.moniepoint.storage.system.engine

import com.moniepoint.storage.system.contract.StorageEngineInterface
import com.moniepoint.storage.system.engine.memtable.MemTable
import com.moniepoint.storage.system.engine.sstable.SSTable
import com.moniepoint.storage.system.engine.wal.WriteAheadLog
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

@Singleton
class StorageSystemEngine(
    @property:Property(name = "storage.root-dir") private val rootDirPath: String,
) : StorageEngineInterface {
    private val rootDir = File(rootDirPath).apply { if (!exists()) mkdirs() }
    private val wal = WriteAheadLog(File(rootDir, "current.wal"))
    private var memTable = MemTable(128 * 1024 * 1024) // setting 124MB as default

    private val ssTables = CopyOnWriteArrayList<SSTable>()

    init {
        val files = rootDir.listFiles { _, name -> name.endsWith(".db") }
        files?.sortedByDescending { it.lastModified() }?.forEach { file ->
            ssTables.add(SSTable(file))
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
        val fileName = "ssTable-${System.currentTimeMillis()}.db"
        val tempFile = File(rootDir, "$fileName.tmp")
        val finalFile = File(rootDir, fileName)

        val newTable = SSTable(tempFile)

        // Write MemTable to temporary file
        newTable.write(memTable.retrieveSortedEntries())

        // Atomic Rename
        tempFile.renameTo(finalFile)

        // Update memory state
        ssTables.add(0, SSTable(finalFile))

        // Safe to clear MemTable and WAL now
        memTable.clear()
        wal.clear()
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
        if (memTable.containsKey(key) || memTable.get(key) != null) {
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
