# Moniepoint LSM-Storage Engine

This project is a high-performance, persistent Key-Value storage engine based on the **Log-Structured Merge-Tree (LSM-Tree)** architecture. It is designed to handle high-write throughput while maintaining durability and efficient memory usage, meeting the rigorous requirements for modern distributed systems.

---

## Project Structure

```text
src/
├── main/
│   ├── kotlin/com/moniepoint/storage/system/
│   │   ├── contract/          # StorageEngineInterface (Abstractions)
│   │   ├── controller/        # Micronaut REST Endpoints (HTTP Layer)
│   │   ├── engine/            # The Core LSM Logic
│   │   │   ├── bloomfilter/   # Probabilistic data structures (Read Optimization)
│   │   │   ├── memtable/      # In-memory SkipList (Write Buffer)
│   │   │   ├── sstable/       # Sorted String Tables (Disk Persistence)
│   │   │   └── wal/           # Write-Ahead Log (Durability & Recovery)
│   │   └── models/            # DTOs and Data Classes
│   └── resources/
│       └── application.yml    # Configuration (root-dir, thresholds)
└── test/
    └── kotlin/com/moniepoint/storage/system/
        ├── StorageSystemBasicFunctionalTest.kt # API & Logic tests
        └── StorageSystemRequirementsTest.kt    # Requirements & SLA verification
        └── StorageSystemErrorsFunctionalTest.kt # API & Logic tests for error cases
```

## How to Run

### 1. Local Mode

Ensure you have JDK 17+ installed.

```declarative
# Build the project
./gradlew build

# Run the application
./gradlew run
```
The server will start on http://localhost:8080. Data is stored in the directory defined in application.yml (default: ./storage).


### 2. Docker Mode
This project includes a multi-stage Dockerfile for optimized builds.

```declarative
# Build the image and run
docker-compose up -d --build
```

## CURL API Documentation

### Put a Key-Value Pair

```declarative
curl -X PUT "http://localhost:8080/v1/storage-system" \
     -H "Content-Type: application/json" \
     -d '{"key": "test_key", "value": "test_value"}'
```

### Get a Value
```declarative
curl -X GET "http://localhost:8080/v1/storage-system/test_key"
```

### Batch Put
```declarative
curl -X PUT "http://localhost:8080/v1/storage-system/batch" \
     -H "Content-Type: application/json" \
     -d '{"items": [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]}'
```

### Range Scan
```declarative
curl -X GET "http://localhost:8080/v1/storage-system/range/a/z"
```

### Delete a Key (Tombstone)
```declarative
curl -X DELETE "http://localhost:8080/v1/storage-system/user_123"
```


## Architecture & Trade-Offs

### **The LSM-Tree Design**

This engine implements a "Write-Optimized" architecture. To ensure high performance, it utilizes a multi-layered approach to data management.
1. **Write-Ahead Log (WAL):** Every operation is first appended to the WAL with a CRC32 Checksum to ensure zero data loss during crashes.
2. **MemTable**: An in-memory ConcurrentSkipListMap provides $O(\log n)$ writes and keeps data sorted for range scans.
3. **SSTables:** Once the MemTable exceeds the size threshold, data is flushed to disk as an immutable Sorted String Table.


#### Engineering Trade-Offs

| Feature | Trade-off | Rationale |
| :--- | :--- | :--- |
| **Sequential I/O** | Appending vs. In-place Updates | We prioritize sequential disk writes (WAL/SSTable) because they are orders of magnitude faster than random-access updates, maximizing throughput. |
| **Read Latency** | Bloom Filters & Sparse Index | To solve "Read Amplification" inherent in LSM-trees, we use Bloom Filters to skip unnecessary disk I/O and a Sparse Index to jump directly to data blocks. |
| **Durability** | CRC32 Verification | We trade a negligible amount of CPU cycles to calculate checksums, ensuring that partial or corrupted writes are never recovered after a crash. |
| **Predictability** | Memory Capping | The engine enforces a strict memory limit on the MemTable. This ensures the system remains stable and predictable regardless of the total dataset size. |