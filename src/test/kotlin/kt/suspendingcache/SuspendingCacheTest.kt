package kt.suspendingcache

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kt.suspendingcache.exceptions.TestException
import kt.suspendingcache.utils.FakeClock
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class SuspendingCacheTest {

    @Test
    fun `missing cached value is initialized using loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)
        val hadEntryBefore = cache.exists("key")
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            delay(100)
            "value"
        }

        // when
        val cached = cache.get("key", loader = loader)

        // then
        assertEquals("value", cached)
        assertEquals(1, counter.get())
        assertEquals(false, hadEntryBefore)
        assertEquals(true, cache.exists("key"))
    }

    @Test
    fun `valid cached value is returned without invoking the loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            delay(100)
            "value"
        }
        cache.get("key", loader = loader)

        // when
        val first = cache.get("key", loader = loader)
        val second = cache.get("key", loader = loader)

        // then
        assertEquals("value", first)
        assertEquals("value", second)
        assertEquals(1, counter.get())
    }

    @Test
    fun `expired value is reloaded`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(clock = clock)
        val counter = AtomicInteger(0)
        val loader: suspend () -> String = {
            counter.incrementAndGet()
            delay(100)
            "new-value-$counter"
        }
        val cached = cache.get("key", 5.minutes, loader)

        // when
        clock.adjust(6.minutes)
        val updated = cache.get("key", 5.minutes, loader)

        // then
        assertEquals(2, counter.get())
        assertEquals("new-value-1", cached)
        assertEquals("new-value-2", updated)
    }

    @Test
    fun `reloaded value is fetched from cache without invoking loader`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(clock = clock, ttl = 5.minutes)
        val counter = AtomicInteger(0)
        val loader: suspend () -> String = {
            counter.incrementAndGet()
            delay(100)
            "new-value-$counter"
        }
        cache.get("key", loader)
        clock.adjust(6.minutes)
        cache.get("key", loader)

        // when
        clock.adjust(1.minutes)
        val cached = cache.get("key", loader)

        // then
        assertEquals(2, counter.get())
        assertEquals("new-value-2", cached)
    }

    @Test
    fun `single loader is used for concurrent requests`() = runTest {
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val loader = suspend {
            started.incrementAndGet()
            delay(100)
            "value"
        }
        val results =
            List(10) {
                backgroundScope.async {
                    cache.get("key", loader = loader)
                }
            }.awaitAll()


        assertEquals(1, started.get())
        results.forEach { result -> assertEquals("value", result) }
        assertEquals(100, currentTime)
    }

    @Test
    fun `when cache is cleared running loaders are canceled`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)

        val cached = backgroundScope.async {
            cache.get("key") {
                started.incrementAndGet()
                delay(100)
                completed.incrementAndGet()
                "value"
            }
        }

        val cancelled = List(10) { index ->
            backgroundScope.async {
                cache.get("key-$index") {
                    started.incrementAndGet()
                    delay(10_000)
                    completed.incrementAndGet()
                    "value-$index"
                }
            }
        }

        delay(200)

        // when
        cache.clear()
        cached.join()
        cancelled.joinAll()

        // then
        assertEquals(11, started.get())
        assertEquals(1, completed.get())
        assertEquals("value", cached.await())
        cancelled.forEach { assertEquals(null, it.await()) }
    }

    @Test
    fun `retry loader after failure`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val attempts = AtomicInteger(0)
        val loader: suspend () -> String = {
            attempts.incrementAndGet()
            delay(50)
            if (attempts.get() == 1) throw TestException()
            "ok-$attempts"
        }

        // when
        val cached = cache.get("key", loader = loader)

        // then
        assertEquals("ok-2", cached)
        assertEquals(2, attempts.get())
    }

    @Test
    fun `return null after loader max retry attempts`() = runTest {
        // given
        val executor = CacheLoadExecutor(maxAttempts = 2)
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher, executor = executor)

        val attempts = AtomicInteger(0)
        val loader: suspend () -> String = {
            attempts.incrementAndGet()
            delay(50)
            throw TestException()
        }

        // when
        val cached = cache.get("key", loader = loader)

        // then
        assertEquals(null, cached)
        assertEquals(2, attempts.get())
    }

    @Test
    fun `refresh of missing key throws CacheNotFoundException`() = runTest {
        val cache = SuspendingCache()
        assertFailsWith<CacheNotFoundException> {
            cache.refresh<String, Any>("missing-key")
        }
    }

    @Test
    fun `get of missing key throws CacheNotFoundException`() = runTest {
        val cache = SuspendingCache()
        assertFailsWith<CacheNotFoundException> {
            cache.get<String, Any>("missing-key")
        }
    }

    @Test
    fun `get returns previous initialized cache by put`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("key", {"value"})

        // when
        val cached = cache.get<String, String>("key")

        // then
        assertEquals("value", cached)
    }

    @Test
    fun `invalidate causes loader to run again on next get`() = runTest {
        val cache = SuspendingCache()
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            delay(10)
            "v-$counter"
        }

        val first = cache.get("k", loader = loader)
        cache.invalidate("k")
        val second = cache.get("k", loader = loader)

        assertEquals("v-1", first)
        assertEquals("v-2", second)
        assertEquals(2, counter.get())
    }

    @Test
    fun `max size first evicts expired entry`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(maxSize = 2, clock = clock)

        cache.get("a", ttl = 2.minutes) { "value-a" }
        cache.get("b", ttl = 5.minutes) { "value-b" }
        val hasValueABefore = cache.exists("a")

        // when
        clock.adjust(3.minutes)
        cache.get("c", ttl = 2.minutes) { "value-c" }

        // then
        assertTrue(hasValueABefore)
        assertFalse(cache.exists("a"))
    }

    @Test
    fun `max size evicts least recently used entry`() = runTest {
        // given
        val cache = SuspendingCache(maxSize = 2)
        val loaders = mutableMapOf<String, AtomicInteger>()

        fun buildLoader(key: String): suspend () -> String = {
            loaders[key]?.incrementAndGet() ?: AtomicInteger(1).also { loaders[key] = it }
            delay(10)
            "value-$key-${loaders[key]}"
        }
        cache.get("a", loader = buildLoader("a"))
        cache.get("b", loader = buildLoader("b"))

        // when
        cache.get("c", loader = buildLoader("c"))
        val a = cache.get("a", loader = buildLoader("a"))

        assertEquals("value-a-2", a)
        assertEquals(2, loaders["a"]?.get())
    }

    @Test
    fun `get of removed item throws CacheNotFoundException`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("key") { "value" }

        // when
        cache.remove("key")

        // then
        assertFailsWith<CacheNotFoundException> {
            cache.get<String, Any>("key")
        }
    }

    @Test
    fun `cancelling one of multiple awaiters does not cancel loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val loader = suspend {
            started.incrementAndGet()
            delay(200)
            completed.incrementAndGet()
            "value"
        }

        val job1 = backgroundScope.async { cache.get("key", loader = loader) }
        val job2 = backgroundScope.async { cache.get("key", loader = loader) }

        // when: cancel one awaiter while another still waits
        delay(50)
        job1.cancel()

        // then: loader keeps running for the remaining awaiter
        val result2 = job2.await()
        assertEquals("value", result2)
        assertTrue(job1.isCancelled)
        assertEquals(1, started.get())
        assertEquals(1, completed.get())
    }

    @Test
    fun `cancelling sole awaiter cancels loader and next get restarts loading`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val loader = suspend {
            val n = started.incrementAndGet()
            if (n == 1) delay(10_000) else delay(100)
            completed.incrementAndGet()
            "ok-$n"
        }

        val job = backgroundScope.async { cache.get("k", loader = loader) }

        // when: cancel the only awaiter
        delay(50)
        job.cancel()

        // then: first caller is cancelled and underlying load gets cancelled too
        assertFailsWith<CancellationException> { job.await() }
        assertEquals(1, started.get())
        assertEquals(0, completed.get())

        // and: a subsequent get restarts the loading and completes successfully
        val value = cache.get("k", loader = loader)
        assertEquals("ok-2", value)
        assertEquals(2, started.get())
        assertEquals(1, completed.get())
    }

    @Test
    fun `concurrent awaiters receive null when loader fails after all retries`() = runTest {
        // given
        val executor = CacheLoadExecutor(maxAttempts = 2)
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher, executor = executor)

        val attempts = AtomicInteger(0)
        val loader: suspend () -> String = {
            val n = attempts.incrementAndGet()
            delay(50)
            if (n <= 2) throw TestException() else "recovered"
        }

        // when: multiple awaiters share the same failing load (after retries it still fails)
        val results = coroutineScope {
            List(5) { async { cache.get("err", loader = loader) } }.awaitAll()
        }

        // then: all awaiters get null (failure not cached as a value)
        results.forEach { r -> assertEquals(null, r) }
        assertEquals(2, attempts.get())

        // and: a subsequent get triggers a fresh load and can succeed
        val recovered = cache.get("err", loader = loader)
        assertEquals("recovered", recovered)
        assertEquals(3, attempts.get())
    }

    @Test
    fun `cancelled loader is restarted if new request arrives`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val loader = suspend {
            val n = started.incrementAndGet()
            delay(if (n == 1) 10_000 else 100)
            completed.incrementAndGet()
            "value-$n"
        }

        val job = backgroundScope.async { cache.get("key", loader = loader) }
        delay(50)
        job.cancel()
        advanceUntilIdle()

        // when: new request arrives after cancellation
        val result = cache.get("key", loader = loader)

        // then: loader restarts and completes successfully
        assertEquals("value-2", result)
        assertEquals(2, started.get())
        assertEquals(1, completed.get())
    }

    @Test
    fun `refresh returns cached value within throttle window`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(ioDispatcher = testDispatcher, refreshThrottle = 300, clock = clock)
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            delay(10)
            "value-${counter.get()}"
        }

        cache.get("key", loader = loader)

        clock.adjust(100.milliseconds)

        // when
        val refreshed = cache.refresh<String, String>("key")

        // then
        assertEquals("value-1", refreshed)
        assertEquals(1, counter.get())
    }

    @Test
    fun `refresh invalidates cache and reloads value after throttle period`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(ioDispatcher = testDispatcher, refreshThrottle = 300, clock = clock)
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            delay(10)
            "value-${counter.get()}"
        }

        cache.get("key", loader = loader)

        clock.adjust(1.seconds)

        // when
        val refreshed = cache.refresh<String, String>("key")

        // then
        assertEquals("value-2", refreshed)
        assertEquals(2, counter.get())
    }

    @Test
    fun `multiple concurrent refreshes use single loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(ioDispatcher = testDispatcher, refreshThrottle = 300, clock = clock)
        val started = AtomicInteger(0)
        val loader = suspend {
            started.incrementAndGet()
            delay(100)
            "value-${started.get()}"
        }
        cache.get("key", loader = loader)

        clock.adjust(1.seconds)

        // when: multiple concurrent refreshes
        val results = coroutineScope {
            List(5) {
                async { cache.refresh<String, String>("key") }
            }.awaitAll()
        }

        // then: single loader execution
        assertEquals(2, started.get()) // 1 initial + 1 refresh
        results.forEach { assertEquals("value-2", it) }
    }

    @Test
    fun `put with same key invalidates previous value`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("key") { "old-value" }
        val first = cache.get<String, String>("key")

        // when
        cache.put("key") { "new-value" }
        val second = cache.get<String, String>("key")

        // then
        assertEquals("old-value", first)
        assertEquals("new-value", second)
    }

    @Test
    fun `put cancels running loader for same key`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val job = backgroundScope.async {
            cache.get("key") {
                started.incrementAndGet()
                delay(10_000)
                completed.incrementAndGet()
                "old-value"
            }
        }

        delay(50)

        // when
        cache.put("key") {
            started.incrementAndGet()
            delay(100)
            completed.incrementAndGet()
            "new-value"
        }

        job.join()
        val value = cache.get<String, String>("key")

        // then
        assertEquals("new-value", value)
        assertEquals(2, started.get())
        assertEquals(1, completed.get()) // only the second loader completed
    }

    @Test
    fun `exception in loader during concurrent access returns null to all awaiters`() = runTest {
        // given
        val executor = CacheLoadExecutor(maxAttempts = 1)
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher, executor = executor)

        val attempts = AtomicInteger(0)
        val loader: suspend () -> String = {
            attempts.incrementAndGet()
            delay(50)
            throw TestException()
        }

        // when: multiple concurrent requests
        val results = coroutineScope {
            List(10) { async { cache.get("key", loader = loader) } }.awaitAll()
        }

        // then: all get null
        results.forEach { assertEquals(null, it) }
        assertEquals(1, attempts.get()) // single attempt
    }

    @Test
    fun `remove prevents get without loader`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("key") { "value" }
        cache.remove("key")

        // when/then
        assertFailsWith<CacheNotFoundException> {
            cache.get<String, String>("key")
        }
    }

    @Test
    fun `remove of running loader cancels it`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val job = backgroundScope.async {
            cache.get("key") {
                started.incrementAndGet()
                delay(10_000)
                completed.incrementAndGet()
                "value"
            }
        }

        delay(50)

        // when
        cache.remove("key")
        job.join()

        // then
        assertEquals(1, started.get())
        assertEquals(0, completed.get())
        assertFailsWith<CacheNotFoundException> {
            cache.get<String, String>("key")
        }
    }

    @Test
    fun `invalidate allows value reload on next get`() = runTest {
        // given
        val cache = SuspendingCache()
        val counter = AtomicInteger(0)
        val loader = suspend {
            counter.incrementAndGet()
            "value-${counter.get()}"
        }

        val first = cache.get("key", loader = loader)
        cache.invalidate("key")
        val second = cache.get("key", loader = loader)

        // then
        assertEquals("value-1", first)
        assertEquals("value-2", second)
        assertEquals(2, counter.get())
    }

    @Test
    fun `invalidate of running loader cancels it`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val job = backgroundScope.async {
            cache.get("key") {
                started.incrementAndGet()
                delay(10_000)
                completed.incrementAndGet()
                "value"
            }
        }

        delay(50)

        // when
        cache.invalidate("key")
        job.join()

        // then: loader was cancelled
        assertEquals(1, started.get())
        assertEquals(0, completed.get())
    }

    @Test
    fun `clear cancels all running loaders and removes all entries`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)

        val jobs = List(5) { index ->
            backgroundScope.async {
                cache.get("key-$index") {
                    started.incrementAndGet()
                    delay(10_000)
                    completed.incrementAndGet()
                    "value-$index"
                }
            }
        }

        delay(100)

        // when
        cache.clear()
        jobs.joinAll()

        // then: all loaders cancelled
        assertEquals(5, started.get())
        assertEquals(0, completed.get())
        assertFalse(cache.exists("key-0"))
        assertFalse(cache.exists("key-1"))
    }

    @Test
    fun `expired value causes immediate reload without returning stale data`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(clock = clock, ttl = 5.minutes)

        val counter = AtomicInteger(0)
        val loader: suspend () -> String = {
            counter.incrementAndGet()
            "value-${counter.get()}"
        }

        val first = cache.get("key", loader = loader)
        clock.adjust(6.minutes)

        // when
        val second = cache.get("key", loader = loader)

        // then: no stale data returned
        assertEquals("value-1", first)
        assertEquals("value-2", second)
        assertEquals(2, counter.get())
    }

    @Test
    fun `concurrent gets on expired value share single reload`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(clock = clock, ttl = 5.minutes, ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val loader: suspend () -> String = {
            started.incrementAndGet()
            delay(100)
            "value-${started.get()}"
        }

        cache.get("key", loader = loader)
        clock.adjust(6.minutes)

        // when: concurrent gets after expiration
        val results = coroutineScope {
            List(10) { async { cache.get("key", loader = loader) } }.awaitAll()
        }

        // then: single reload
        assertEquals(2, started.get()) // 1 initial + 1 reload
        results.forEach { assertEquals("value-2", it) }
    }

    @Test
    fun `loader throwing CancellationException is not retried`() = runTest {
        // given
        val executor = CacheLoadExecutor(maxAttempts = 5)
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher, executor = executor)

        val attempts = AtomicInteger(0)
        val loader: suspend () -> String = {
            attempts.incrementAndGet()
            throw CancellationException("test cancellation")
        }

        // when
        val result = cache.get("key", loader = loader)

        // then: no retries for CancellationException
        assertEquals(null, result)
        assertEquals(1, attempts.get())
    }

    @Test
    fun `max size eviction happens before adding new entry`() = runTest {
        // given
        val cache = SuspendingCache(maxSize = 2)
        cache.get("a") { "value-a" }
        cache.get("b") { "value-b" }

        // when: adding third entry
        cache.get("c") { "value-c" }

        // then: one entry was evicted
        val existsCount = listOf("a", "b", "c").count { cache.exists(it) }
        assertEquals(2, existsCount)
    }

    @Test
    fun `evicted entry cannot be retrieved without loader`() = runTest {
        // given
        val cache = SuspendingCache(maxSize = 2)
        cache.get("a") { "value-a" }
        cache.get("b") { "value-b" }
        cache.get("c") { "value-c" }

        // when: try to get evicted entry without loader
        val results = listOf(
            kotlin.runCatching { cache.get<String, String>("a") },
            kotlin.runCatching { cache.get<String, String>("b") },
            kotlin.runCatching { cache.get<String, String>("c") }
        )

        // then: at least one fails with CacheNotFoundException
        val exceptions = results.count { it.isFailure && it.exceptionOrNull() is CacheNotFoundException }
        assertTrue(exceptions >= 1)
    }

    @Test
    fun `accessing entry updates its LRU timestamp`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(maxSize = 2, ioDispatcher = testDispatcher, clock = clock)
        cache.get("a") { "value-a" }
        delay(10)
        clock.adjust(10.milliseconds)
        cache.get("b") { "value-b" }
        delay(10)
        clock.adjust(10.milliseconds)

        // when: access 'a' to make it most recently used
        cache.get("a") { "value-a" }
        delay(10)
        clock.adjust(10.milliseconds)
        cache.get("c") { "value-c" }

        // then: 'b' should be evicted (least recently used)
        assertTrue(cache.exists("a"))
        assertFalse(cache.exists("b"))
        assertTrue(cache.exists("c"))
    }

    @Test
    fun `exception always returns null and does not cache failure`() = runTest {
        // given
        val executor = CacheLoadExecutor(maxAttempts = 1)
        val cache = SuspendingCache(executor = executor)
        val counter = AtomicInteger(0)
        val loader: suspend () -> String = {
            val n = counter.incrementAndGet()
            delay(10)
            if (n <= 2) throw TestException("attempt-$n")
            "success"
        }

        // when: first call fails
        val first = cache.get("key", loader = loader)
        // and: second call also starts fresh (failure not cached)
        val second = cache.get("key", loader = loader)
        // and: third call succeeds
        val third = cache.get("key", loader = loader)

        // then: failures return null, success is cached
        assertEquals(null, first)
        assertEquals(null, second)
        assertEquals("success", third)
        assertEquals(3, counter.get())
    }

    @Test
    fun `exception during expiration check does not crash cache`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.get("key1") { "value1" }

        // when: simulate concurrent access during invalidation
        coroutineScope {
            List(10) { index ->
                async {
                    if (index % 2 == 0) {
                        cache.invalidate("key1")
                    } else {
                        cache.get("key1") { "value1-reload" }
                    }
                }
            }.awaitAll()
        }

        // then: cache still functional
        assertTrue(cache.exists("key1"))
    }

    @Test
    fun `cancellation of all awaiters before loader completes cancels loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val loader = suspend {
            started.incrementAndGet()
            delay(10_000)
            completed.incrementAndGet()
            "value"
        }

        val jobs = List(5) {
            backgroundScope.async { cache.get("key", loader = loader) }
        }

        delay(50)

        // when: cancel all awaiters
        jobs.forEach { it.cancel() }
        jobs.joinAll()

        // then: loader was cancelled
        assertEquals(1, started.get())
        assertEquals(0, completed.get())

        // and: subsequent get restarts loader
        val value = cache.get("key", loader = loader)
        assertEquals("value", value)
        assertEquals(2, started.get())
        assertEquals(1, completed.get())
    }

    @Test
    fun `partial cancellation of awaiters does not cancel loader`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val loader = suspend {
            started.incrementAndGet()
            delay(200)
            completed.incrementAndGet()
            "value"
        }

        val jobs = List(10) {
            backgroundScope.async { cache.get("key", loader = loader) }
        }

        delay(50)

        // when: cancel only some awaiters
        jobs.take(7).forEach { it.cancel() }

        // then: loader completes for remaining awaiters
        val remainingResults = jobs.drop(7).awaitAll()
        remainingResults.forEach { assertEquals("value", it) }
        assertEquals(1, started.get())
        assertEquals(1, completed.get())
    }

    @Test
    fun `different keys load independently`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        val started = mutableMapOf<String, AtomicInteger>()
        fun loader(key: String): suspend () -> String = {
            started.getOrPut(key) { AtomicInteger(0) }.incrementAndGet()
            delay(100)
            "value-$key"
        }

        // when: concurrent loads for different keys
        val results = coroutineScope {
            (1..5).map { index ->
                async { cache.get("key-$index", loader = loader("key-$index")) }
            }.awaitAll()
        }

        // then: each key loaded once
        assertEquals(5, started.size)
        started.values.forEach { assertEquals(1, it.get()) }
        results.forEachIndexed { index, value ->
            assertEquals("value-key-${index + 1}", value)
        }
    }

    @Test
    fun `TTL per-request overrides default TTL`() = runTest {
        // given
        val instant = Instant.parse("2025-12-15T12:00:00Z")
        val clock = FakeClock(instant)
        val cache = SuspendingCache(clock = clock, ttl = 10.minutes)

        val counter = AtomicInteger(0)
        val loader: suspend () -> String = {
            counter.incrementAndGet()
            "value-${counter.get()}"
        }

        // when: use per-request TTL shorter than default
        cache.get("key", ttl = 2.minutes, loader = loader)
        clock.adjust(3.minutes)
        cache.get("key", ttl = 2.minutes, loader = loader)

        // then: value expired based on per-request TTL
        assertEquals(2, counter.get())
    }

    @Test
    fun `exists returns false for evicted entry`() = runTest {
        // given
        val cache = SuspendingCache(maxSize = 1)
        cache.get("a") { "value-a" }

        // when
        cache.get("b") { "value-b" }

        // then
        assertFalse(cache.exists("a"))
        assertTrue(cache.exists("b"))
    }

    @Test
    fun `exists returns false after remove`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("key") { "value" }
        assertTrue(cache.exists("key"))

        // when
        cache.remove("key")

        // then
        assertFalse(cache.exists("key"))
    }

    @Test
    fun `exists returns false after clear`() = runTest {
        // given
        val cache = SuspendingCache()
        cache.put("a") { "value-a" }
        cache.put("b") { "value-b" }

        // when
        cache.clear()

        // then
        assertFalse(cache.exists("a"))
        assertFalse(cache.exists("b"))
    }

    @Test
    fun `multiple loaders should run in parallel`() = runTest {
        // given
        val testDispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val cache = SuspendingCache(ioDispatcher = testDispatcher)

        // when
        val cached1 = backgroundScope.async { cache.get("key1") { delay(100).let { "value1" } } }
        val cached2 = backgroundScope.async { cache.get("key2") { delay(100).let { "value2" } } }
        joinAll(cached1, cached2)

        // then
        assertEquals(100, currentTime)
    }
}

