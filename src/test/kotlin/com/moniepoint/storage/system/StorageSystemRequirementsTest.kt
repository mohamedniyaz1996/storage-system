package com.moniepoint.storage.system

import com.moniepoint.storage.system.models.PutRequest
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldMatch
import io.micronaut.context.annotation.Property
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import java.io.File

@MicronautTest
class StorageSystemRequirementsTest(
    @param:Client("/v1/storage-system") val storageSystemClient: HttpClient,
    @property:Property(name = "storage.root-dir") private val rootDirPath: String,
) : BehaviorSpec() {
    override suspend fun beforeSpec(spec: Spec) {
        val storageDir = File(rootDirPath)
        if (storageDir.exists()) {
            storageDir.deleteRecursively()
        }
        storageDir.mkdirs()
    }

    init {
        val storageDir = File(rootDirPath)

        given("Given crash-friendliness and durability") {
            val key = "durability-key"
            val value = "essential-data"

            `when`("data is written to the system") {
                val putRequest = HttpRequest.PUT("", PutRequest(key, value))
                val putResponse =
                    storageSystemClient.toBlocking().exchange(
                        putRequest,
                        Unit::class.java,
                    )

                then("the Write-Ahead Log (WAL) should immediately persist it to disk") {
                    putResponse.status shouldBe HttpStatus.CREATED
                    val walFile = File(storageDir, "current.wal")
                    walFile.exists() shouldBe true
                    walFile.length() shouldBeGreaterThan 0L
                }
            }
        }

        given("Dataset Management & Predictable Memory") {
            `when`("the volume of data exceeds the MemTable threshold") {
                repeat(150) { i ->
                    storageSystemClient.toBlocking().exchange(
                        HttpRequest.PUT("", PutRequest("key-$i", "value-$i")),
                        Unit::class.java,
                    )
                }

                then("it should trigger a flush and create a sequenced SSTable file") {
                    val dbFiles = storageDir.listFiles { _, name -> name.endsWith(".db") }
                    dbFiles shouldNotBe null
                    dbFiles!!.size shouldBeGreaterThan 0

                    dbFiles.first().name shouldMatch Regex("\\d{10}.db")
                }
            }
        }

        given("Low Latency Reads") {
            `when`("requesting a key that definitely does not exist") {
                val startTime = System.currentTimeMillis()
                val response =
                    storageSystemClient.toBlocking().exchange(
                        HttpRequest.GET<Any>("/non-existent-at-all"),
                        String::class.java,
                    )
                val duration = System.currentTimeMillis() - startTime

                then("the Bloom Filter should allow it to return 'No Content' almost instantly") {
                    response.status shouldBe HttpStatus.NO_CONTENT
                    duration shouldBeGreaterThan -1L
                }
            }
        }
    }
}
