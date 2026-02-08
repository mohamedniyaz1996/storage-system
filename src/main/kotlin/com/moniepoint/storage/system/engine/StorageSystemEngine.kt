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
    @property:Property(name = "storage.root-dir:data") private val rootDirPath: String,
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
