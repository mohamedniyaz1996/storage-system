package com.moniepoint.storage.system.models

import io.micronaut.core.annotation.Introspected
import io.micronaut.serde.annotation.Serdeable
import jakarta.validation.constraints.NotBlank

@Introspected
@Serdeable
data class PutRequest(
    @field:NotBlank val key: String,
    @field:NotBlank val value: String,
)

@Introspected
@Serdeable
data class BatchPutRequest(
    val items: List<PutRequest>,
)
