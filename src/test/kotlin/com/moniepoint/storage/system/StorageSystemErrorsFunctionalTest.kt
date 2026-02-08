package com.moniepoint.storage.system

import com.moniepoint.storage.system.models.PutRequest
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest

@MicronautTest
class StorageSystemErrorsFunctionalTest(
    @param:Client("/v1/storage-system") val storageSystemClient: HttpClient,
) : FunSpec() {
    init {

        test("should return 500 when engine fails on PUT in-case of empty key value pair") {
            val key = ""
            val value = ""
            val requestPayload = PutRequest(key, value)

            val putRequest = HttpRequest.PUT("", requestPayload)
            val exception =
                shouldThrow<HttpClientResponseException> {
                    storageSystemClient.toBlocking().exchange(
                        putRequest,
                        Unit::class.java,
                    )
                }
            exception.status shouldBe HttpStatus.BAD_REQUEST
        }
    }
}
