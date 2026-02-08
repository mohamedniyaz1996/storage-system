package com.moniepoint.storage.system.controller

import com.moniepoint.storage.system.engine.StorageSystemEngine
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Delete
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Put
import java.util.Base64


@Controller("/v1/storage-system")
class StorageSystemController(
    private val storageSystemEngine: StorageSystemEngine
) {

    @Put("/{key}")
    fun put(@PathVariable key: String, @Body value: String) {
        val bytes = Base64.getDecoder().decode(value)
        storageSystemEngine.put(key, bytes)
    }

    @Get("/{key}")
    fun get(@PathVariable key: String): String? {
        val result = storageSystemEngine.read(key) ?: return null
        return Base64.getEncoder().encodeToString(result)
    }

    @Delete("/{key}")
    fun delete(@PathVariable key: String) = storageSystemEngine.delete(key)
}