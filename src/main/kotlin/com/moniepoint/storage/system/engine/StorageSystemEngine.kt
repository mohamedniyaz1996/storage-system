package com.moniepoint.storage.system.engine

import com.moniepoint.storage.system.contract.StorageEngineInterface
import com.moniepoint.storage.system.engine.memtable.MemTable
import com.moniepoint.storage.system.engine.sstable.SSTable
import com.moniepoint.storage.system.engine.wal.WriteAheadLog
import jakarta.inject.Singleton
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

@Singleton
class StorageSystemEngine : StorageEngineInterface {
    private val rootDir = File("data").apply { if (!exists()) mkdirs() }
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
        val newTable = SSTable(File(rootDir, fileName))

        newTable.write(memTable.retrieveSortedEntries())

        ssTables.add(0, newTable)
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
        val ramResult = memTable.get(key)
        if (ramResult != null) return ramResult

        for (table in ssTables) {
            val diskResult = table.get(key)
            if (diskResult != null) return diskResult
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
