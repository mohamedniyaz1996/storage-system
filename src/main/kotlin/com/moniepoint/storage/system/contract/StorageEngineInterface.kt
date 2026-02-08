package com.moniepoint.storage.system.contract

interface StorageEngineInterface {
    fun put(
        key: String,
        value: ByteArray,
    )

    fun batchPut(items: Map<String, ByteArray>)

    fun read(key: String): ByteArray?

    fun readKeyRange(
        startKey: String,
        endKey: String,
    ): List<Pair<String, ByteArray?>>

    fun delete(key: String)
}
