package com.moniepoint.storage.system.engine

import com.moniepoint.storage.system.engine.memtable.MemTable
import com.moniepoint.storage.system.engine.sstable.SSTable
import com.moniepoint.storage.system.engine.wal.WriteAheadLog
import com.moniepoint.storage.system.`interface`.StorageEngineInterface
import jakarta.inject.Singleton
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

@Singleton
class StorageEngine: StorageEngineInterface {

    private val rootDir = File("data").apply { if(!exists()) mkdirs() }
    private val wal= WriteAheadLog(File(rootDir, "current.wal"))
    private var memTable = MemTable(128 * 1024 * 1024) // setting 124MB as default

    private val ssTables = CopyOnWriteArrayList<SSTable>()

    private fun flushMemTable() {
        val fileName = "ssTable-${System.currentTimeMillis()}.db"
        val newTable = SSTable(File(rootDir, fileName))

        newTable.write(memTable.retrieveSortedEntries())

        ssTables.add(0, newTable)
        memTable.clear()
        wal.clear()
    }

    @Synchronized
    override fun put(key: String, value: ByteArray) {
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
        endKey: String
    ): List<Pair<String, ByteArray?>> {
        return memTable.getRange(startKey, endKey)
    }

    override fun delete(key: String) {
        wal.append(key, null, isDelete = true)
        memTable.put(key, null)

        if (memTable.isOverFull()) {
            flushMemTable()
        }
    }
}