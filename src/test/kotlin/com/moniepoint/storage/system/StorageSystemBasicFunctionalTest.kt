package com.moniepoint.storage.system

import com.moniepoint.storage.system.engine.StorageSystemEngine
import com.moniepoint.storage.system.models.BatchPutRequest
import com.moniepoint.storage.system.models.PutRequest
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldMatch
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

@MicronautTest
class StorageSystemBasicFunctionalTest(
    @param:Client("/v1/storage-system") val storageSystemClient: HttpClient,
    private val engine: StorageSystemEngine,
) : FunSpec() {
    override suspend fun beforeTest(testCase: TestCase) {
        engine.resetInternalState()
    }

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
            val getRequest = HttpRequest.GET<Any>("/$randomKey")
            val response =
                storageSystemClient.toBlocking().exchange(
                    getRequest,
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

            val batchPutRequest = HttpRequest.PUT("/batch", requestPayload)
            val batchPutResponse =
                storageSystemClient.toBlocking().exchange(
                    batchPutRequest,
                    Unit::class.java,
                )
            batchPutResponse.status shouldBe HttpStatus.CREATED

            var getRequest = HttpRequest.GET<Any>("/range/a/b")
            var rangeResponse =
                storageSystemClient.toBlocking().retrieve(
                    getRequest,
                    Argument.listOf(Map::class.java),
                )

            rangeResponse.size shouldBe 2

            getRequest = HttpRequest.GET<Any>("/range/a/c")
            rangeResponse =
                storageSystemClient.toBlocking().retrieve(
                    getRequest,
                    Argument.listOf(Map::class.java),
                )

            rangeResponse.size shouldBe 3
        }

        test("should return empty list for out-of-order range scan") {
            val getRequest = HttpRequest.GET<Any>("/range/z/a")
            val results =
                storageSystemClient.toBlocking().retrieve(
                    getRequest,
                    Argument.listOf(Map::class.java),
                )
            results shouldBe emptyList<Any>()
        }

        test("concurrency: should handle rapid updates to the same key and should be able to retrieve the latest updated version") {
            val key = "concurrentKey"
            val versions = listOf("v1", "v2", "v3", "v4", "v5")

            versions.forEach { v ->
                val putRequest = HttpRequest.PUT("", PutRequest(key, v))
                storageSystemClient.toBlocking().exchange(putRequest, Unit::class.java)
            }

            storageSystemClient.toBlocking().retrieve(key) shouldBe "v5"
        }

        test("should handle empty batch put requests gracefully") {
            val emptyPayload = BatchPutRequest(items = emptyList())
            val putRequest = HttpRequest.PUT("/batch", emptyPayload)
            val response =
                storageSystemClient.toBlocking().exchange(
                    putRequest,
                    Unit::class.java,
                )
            response.status shouldBe HttpStatus.CREATED
        }

        test("should return empty list when no keys exist in the provided range") {
            val getRequest = HttpRequest.GET<Any>("/range/m/n")
            val results =
                storageSystemClient.toBlocking().retrieve(
                    getRequest,
                    Argument.listOf(Map::class.java),
                )
            results shouldBe emptyList<Any>()
        }

        test("concurrency: should handle multiple threads writing to the same key") {
            val key = "threadSafeKey"
            val iterations = 100

            runBlocking {
                (1..iterations).map { i ->
                    launch(kotlinx.coroutines.Dispatchers.IO) {
                        val putRequest = HttpRequest.PUT("", PutRequest(key, "val-$i"))
                        storageSystemClient.toBlocking().exchange(
                            putRequest,
                            Unit::class.java,
                        )
                    }
                }.joinAll()
            }

            // The result should be one of the values (no crash/deadlock)
            val finalVal = storageSystemClient.toBlocking().retrieve(key)
            finalVal shouldMatch Regex("val-\\d+")
        }
    }
}
