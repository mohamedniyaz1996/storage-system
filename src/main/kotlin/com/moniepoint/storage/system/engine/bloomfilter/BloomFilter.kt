package com.moniepoint.storage.system.engine.bloomfilter

import java.util.BitSet
import java.util.zip.CRC32

class BloomFilter(private val size: Int, private val numHashFunctions: Int) {
    private val bitSet = BitSet(size)

    fun add(key: String) {
        getHashes(key).forEach { hash ->
            bitSet.set((hash % size).toInt())
        }
    }

    fun mightContain(key: String): Boolean {
        return getHashes(key).all { hash ->
            bitSet.get((hash % size).toInt())
        }
    }

    private fun getHashes(key: String): List<Long> {
        val hashes = mutableListOf<Long>()
        val crc = CRC32()
        for (i in 1..numHashFunctions) {
            crc.reset()
            crc.update("$i$key".toByteArray())
            hashes.add(crc.value)
        }
        return hashes
    }
}
