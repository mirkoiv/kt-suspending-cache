package kt.suspendingcache

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kt.suspendingcache.exceptions.TestException
import kt.suspendingcache.utils.FakeClock
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

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
        val results = coroutineScope {
            List(10) {
                async {
                    cache.get("key", loader = loader)
                }
            }.awaitAll()
        }

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
        cache.put("key", {"value"})

        // when
        cache.remove("key")

        // then
        assertFailsWith<CacheNotFoundException> {
            cache.get<String, Any>("key")
        }
    }
}

