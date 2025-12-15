# Suspending Cache

A Kotlin library implementing a coroutine-friendly suspending cache.

This project was created as a capstone for the [KT Academy Coroutine Mastery Course](https://coroutinesmastery.com/) and shows advanced coroutine patterns for safe, efficient, and concurrent caching.

---

## Key features
- Coroutine-safe loader deduplication (single loader for concurrent requests)
- TTL-based expiration and explicit refresh/invalidate
- Expired or LRU eviction with configurable `maxSize`
- Cancellation of running loaders on clear
- Configurable executor and IO dispatcher
- Automatic retry with exponential backoff for failed loaders
- Limited concurrency for loader invocations

## Usage

Simple example using SuspendingCache:

```kotlin
val cache = SuspendingCache()

suspend fun getUser(userId: String): User? {
    return cache.get(userId) {
        // loader called once for concurrent callers
        fetchUserFromNetwork(userId)
    }
}
```

Behavior highlights:
- ***get(key, loader)*** returns cached value or calls loader if missing/expired
- concurrent calls to ***get*** with same key share a single loader invocation
- ***invalidate(key)*** forces next get to reload
- ***refresh(key)*** reloads an existing key
- ***put(key, loader)*** register a loader for a key without immediate loading

## Configuration

* Set TTL, max size, custom IO dispatcher, and executor via the SuspendingCache constructor:

```kotlin
val cache = SuspendingCache(
    clock = Clock.systemDefaultZone(),
    ttl = Duration.minutes(5),
    executor = CacheLoadExecutor(concurrencyLimit = 4),
    ioDispatcher = Dispatchers.IO,
    maxSize = 500
)
```

### Executor: concurrency limit and retry/backoff

The `CacheLoadExecutor` used by `SuspendingCache` limits how many loader coroutines run in parallel and automatically retries failing loader invocations using exponential backoff.

Key configuration options:
- `concurrencyLimit` — maximum number of loaders running concurrently. Excess loader requests are queued until a slot is free.
- `maxAttempts` — total attempts including the initial try (e.g. `3` means 1 initial + up to 2 retries).
- `initialDelay` — initial backoff delay in milliseconds before the first retry.
- `multiplier` — backoff multiplier applied to the delay on each retry (exponential backoff).
- `maxDelay` — maximum delay in milliseconds between retries.


## References
- [Coroutines Mastery Course](https://coroutinesmastery.com/)
- [Marcin Moskala - KT Academy resources](https://kt.academy)


