package com.moniepoint.storage.system

import com.moniepoint.storage.system.models.BatchPutRequest
import com.moniepoint.storage.system.models.PutRequest
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest

@MicronautTest
class StorageSystemBasicFunctionalTest(
    @param:Client("/v1/storage-system") val storageSystemClient: HttpClient,
) : FunSpec() {
    init {
        test("should successfully store and retrieve a value") {
            val key = "testKey"
            val value = "Hello Moniepoint!"
            val requestPayload = PutRequest(key, value)

            val putRequest = HttpRequest.PUT("", requestPayload)
            val putResponse = storageSystemClient.toBlocking().exchange(putRequest, Unit::class.java)
            putResponse.status shouldBe HttpStatus.CREATED

            val getResponse = storageSystemClient.toBlocking().retrieve(key)
            getResponse shouldBe value
        }

        test("should return 204 No Content for a missing key") {
            val randomKey = "random-key"
            val response =
                storageSystemClient.toBlocking().exchange(
                    HttpRequest.GET<Any>("/$randomKey"),
                    String::class.java,
                )
            response.status shouldBe HttpStatus.NO_CONTENT
        }

        test("should handle deletion correctly (Tombstone logic)") {
            val key = "deletionKey"
            val requestPayload = PutRequest(key, "temporary")
            val putRequest = HttpRequest.PUT("", requestPayload)
            val putResponse = storageSystemClient.toBlocking().exchange(putRequest, Unit::class.java)
            putResponse.status shouldBe HttpStatus.CREATED

            // DELETE
            storageSystemClient.toBlocking().exchange(HttpRequest.DELETE<Any>("/$key"), Unit::class.java)

            // GET after DELETE should be 204
            val getResponse = storageSystemClient.toBlocking().exchange(HttpRequest.GET<Any>("/$key"), String::class.java)
            getResponse.status shouldBe HttpStatus.NO_CONTENT
        }

        test("should perform range scan successfully") {
            val requestPayload =
                BatchPutRequest(
                    items =
                        listOf(
                            PutRequest("a", "1"),
                            PutRequest("b", "2"),
                            PutRequest("c", "3"),
                        ),
                )

            println("Range Request-Payload: $requestPayload")

            val batchPutRequest = HttpRequest.PUT("/batch", requestPayload)
            val batchPutResponse =
                storageSystemClient.toBlocking().exchange(
                    batchPutRequest,
                    Unit::class.java,
                )
            batchPutResponse.status shouldBe HttpStatus.CREATED

            storageSystemClient.toBlocking().retrieve(
                HttpRequest.GET<Any>("/range/a/b"),
                Argument.listOf(Map::class.java),
            ).size shouldBe 2

            storageSystemClient.toBlocking().retrieve(
                HttpRequest.GET<Any>("/range/a/c"),
                Argument.listOf(Map::class.java),
            ).size shouldBe 3
        }

        test("should return empty list for out-of-order range scan") {
            val results =
                storageSystemClient.toBlocking().retrieve(
                    HttpRequest.GET<Any>("/range/z/a"),
                    Argument.listOf(Map::class.java),
                )
            results shouldBe emptyList<Any>()
        }

        test("concurrency: should handle rapid updates to the same key and should be able to retrieve the latest updated version") {
            val key = "concurrentKey"
            val versions = listOf("v1", "v2", "v3", "v4", "v5")

            versions.forEach { v ->
                storageSystemClient.toBlocking().exchange(HttpRequest.PUT("", PutRequest(key, v)), Unit::class.java)
            }

            storageSystemClient.toBlocking().retrieve(key) shouldBe "v5"
        }
    }
}
