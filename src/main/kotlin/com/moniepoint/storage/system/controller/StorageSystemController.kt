package com.moniepoint.storage.system.controller

import com.moniepoint.storage.system.engine.StorageSystemEngine
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Consumes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Delete
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Produces
import io.micronaut.http.annotation.Put

@Controller("/v1/storage-system")
class StorageSystemController(
    private val storageSystemEngine: StorageSystemEngine,
) {
    @Put("/{key}")
    @Consumes("text/plain")
    fun put(
        @PathVariable key: String,
        @Body value: String,
    ): HttpResponse<Unit> {
        return try {
            val bytes = value.toByteArray(Charsets.UTF_8)
            storageSystemEngine.put(key, bytes)
            HttpResponse.status(HttpStatus.CREATED)
        } catch (e: Exception) {
            HttpResponse.serverError()
        }
    }

    @Get("/{key}")
    @Produces("text/plain")
    fun get(
        @PathVariable key: String,
    ): HttpResponse<String> {
        val bytes = storageSystemEngine.read(key)
        return if (bytes == null) {
            HttpResponse.noContent()
        } else {
            HttpResponse.ok(String(bytes))
        }
    }

    @Delete("/{key}")
    fun delete(
        @PathVariable key: String,
    ) = storageSystemEngine.delete(key)

    @Put("/batch")
    @Consumes("application/json")
    fun batchPut(
        @Body items: Map<String, String>,
    ): HttpResponse<Unit> {
        return try {
            val byteItems = items.mapValues { it.value.toByteArray(Charsets.UTF_8) }
            storageSystemEngine.batchPut(byteItems)
            HttpResponse.ok()
        } catch (e: Exception) {
            HttpResponse.serverError()
        }
    }

    @Get("/range/{startKey}/{endKey}")
    @Produces("application/json")
    fun getRange(
        @PathVariable startKey: String,
        @PathVariable endKey: String,
    ): HttpResponse<Any> {
        return try {
            val results = storageSystemEngine.readKeyRange(startKey, endKey)
            val response =
                results.map { (key, value) ->
                    mapOf("key" to key, "value" to (value?.let { String(it) } ?: ""))
                }
            if (response.isEmpty()) {
                HttpResponse.noContent()
            } else {
                HttpResponse.ok(response)
            }
        } catch (e: Exception) {
            HttpResponse.serverError<String>().body("Error retrieving range: ${e.message}")
        }
    }

    @Get("/ping")
    fun isHealthy() = "Application is up and running healthy!!!"
}
