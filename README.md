
# Latest Value Price Service

## Overview

This project implements an in-memory **Latest Value Price Service** in Java.

The service allows:

- **Producers** to publish price data in atomic batches
- **Consumers** to retrieve the latest price for given instrument IDs

The implementation follows production-grade standards:
- Thread-safe
- Atomic batch visibility
- Clean design
- Defensive validation
- Unit tested
- No external runtime dependencies (Java SDK only)

---

## Business Requirements Implemented

### Price Record Structure

Each price record contains:

- `id` — Instrument identifier (String)
- `asOf` — Timestamp indicating when the price was determined
- `payload` — Flexible price data (Object)

The *latest value* is determined by the `asOf` timestamp, not by arrival order.

---

## Batch Lifecycle

Producers must follow this sequence:

1. `startBatch()`
2. `uploadPrices(batchId, records)` (parallel, max 1000 records per chunk)
3. `completeBatch(batchId)` **or** `cancelBatch(batchId)`

### Guarantees

- Consumers never see partial batches
- Completed batches become visible atomically
- Cancelled batches are discarded
- Upload chunk size is validated (max 1000 records)
- Parallel uploads are supported safely

---

## Design Decisions

### 1. Atomic Visibility

A `volatile` snapshot map (`currentPrices`) is replaced atomically when a batch completes.

Consumers always read from a stable, fully committed snapshot.

### 2. Parallel Upload Support

- Each batch uses a `ConcurrentHashMap`
- `merge()` ensures atomic updates per instrument
- No locking during uploads
- Only `completeBatch()` uses a lock to ensure atomic publication

### 3. Thread Safety Strategy

- `ConcurrentHashMap` for data storage
- `ReentrantLock` for batch commit lifecycle
- `volatile` reference for snapshot switching

### 4. Fail-Fast Validation

- Chunk size must be ≤ 1000
- Null checks on input
- Invalid batch usage throws `IllegalStateException`

---

## Project Structure

