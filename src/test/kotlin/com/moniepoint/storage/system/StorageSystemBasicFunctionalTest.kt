package com.moniepoint.storage.system

import com.moniepoint.storage.system.models.PutRequest
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
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

//        test("should perform range scan successfully") {
//            val requestPayload =
//                BatchPutRequest(
//                    items =
//                        listOf(
//                            PutRequest("a", "1"),
//                            PutRequest("b", "2"),
//                            PutRequest("c", "3"),
//                        ),
//                )
//
//            val batchPutRequest = HttpRequest.PUT("/batch", requestPayload)
//            val batchPutResponse =
//                storageSystemClient.toBlocking().exchange(
//                    batchPutRequest,
//                    Unit::class.java,
//                )
//            batchPutResponse.status shouldBe HttpStatus.CREATED
//
//            storageSystemClient.toBlocking().retrieve(
//                HttpRequest.GET<List<Map<String, String>>>("/range/a/b"),
//                List::class.java,
//            ).size shouldBe 2
//
//            storageSystemClient.toBlocking().retrieve(
//                HttpRequest.GET<List<Map<String, String>>>("/range/a/c"),
//                List::class.java,
//            ).size shouldBe 3
//        }
    }
}
