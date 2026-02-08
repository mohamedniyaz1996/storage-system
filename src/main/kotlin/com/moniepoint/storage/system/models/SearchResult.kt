package com.moniepoint.storage.system.models

data class SearchResult(val found: Boolean, val value: ByteArray?) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SearchResult

        if (found != other.found) return false
        if (!value.contentEquals(other.value)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = found.hashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}
