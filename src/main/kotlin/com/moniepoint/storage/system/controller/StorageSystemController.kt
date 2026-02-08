package com.moniepoint.storage.system.controller

import com.moniepoint.storage.system.engine.StorageSystemEngine
import com.moniepoint.storage.system.models.BatchPutRequest
import com.moniepoint.storage.system.models.PutRequest
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
    @Put
    @Consumes(io.micronaut.http.MediaType.APPLICATION_JSON)
    fun put(
        @Body request: PutRequest,
    ): HttpResponse<Unit> {
        return try {
            val bytes = request.value.toByteArray(Charsets.UTF_8)
            storageSystemEngine.put(request.key, bytes)
            HttpResponse.status(HttpStatus.CREATED)
        } catch (e: Exception) {
            HttpResponse.serverError()
        }
    }

    @Get("/{key}")
    @Produces(io.micronaut.http.MediaType.TEXT_PLAIN)
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
    ): HttpResponse<Unit> {
        return try {
            storageSystemEngine.delete(key)
            HttpResponse.status(HttpStatus.OK)
        } catch (e: Exception) {
            HttpResponse.serverError()
        }
    }

    @Put("/batch")
    @Consumes(io.micronaut.http.MediaType.APPLICATION_JSON)
    fun batchPut(
        @Body request: BatchPutRequest,
    ): HttpResponse<Unit> {
        return try {
            val byteItems =
                request.items.associate {
                    it.key to it.value.toByteArray(Charsets.UTF_8)
                }
            storageSystemEngine.batchPut(byteItems)
            HttpResponse.status(HttpStatus.CREATED)
        } catch (e: Exception) {
            HttpResponse.serverError()
        }
    }

    @Get("/range/{startKey}/{endKey}")
    @Produces(io.micronaut.http.MediaType.APPLICATION_JSON)
    fun getRange(
        @PathVariable startKey: String,
        @PathVariable endKey: String,
    ): HttpResponse<Any> {
        if (startKey > endKey) {
            // Return 200 OK with empty list (standard DB behavior)
            return HttpResponse.ok(emptyList<Any>())
        }

        return try {
            val results = storageSystemEngine.readKeyRange(startKey, endKey)
            val response =
                results.map { (key, value) ->
                    mapOf("key" to key, "value" to (value?.let { String(it) } ?: ""))
                }
            if (response.isEmpty()) {
                HttpResponse.ok(emptyList<Any>())
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
