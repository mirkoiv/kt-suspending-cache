package kt.suspendingcache

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kt.suspendingcache.exceptions.TestException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith

class CacheLoadExecutorTest {

    @Test
    fun `successful execution without retry`() = runTest {
        // given
        val exec = CacheLoadExecutor()

        // when
        val result = exec.run { "ok" }

        // then
        assertEquals("ok", result)
    }

    @Test
    fun `retries on failure and eventually succeeds`() = runTest {
        // given
        val exec = CacheLoadExecutor(
            maxAttempts = 3,
            initialDelay = 100,
            multiplier = 2.0,
            maxDelay = 10_000
        )
        val attempts = AtomicInteger(0)
        val deferred = async {
            exec.run {
                val n = attempts.incrementAndGet()
                if (n < 3) throw TestException("fail #$n")
                "done"
            }
        }

        // when
        val result = deferred.await()

        // then
        assertEquals("done", result)
        assertEquals(3, attempts.get())
    }

    @Test
    fun `throws when retries exhausted`() = runTest {
        // given
        val scope = CoroutineScope(SupervisorJob() + this.coroutineContext[CoroutineDispatcher]!!)
        val exec = CacheLoadExecutor(maxAttempts = 2)
        val attempts = AtomicInteger(0)
        val deferred = scope.async {
            exec.run<String> {
                attempts.incrementAndGet()
                throw TestException("permanent failure")
            }
        }

        // when
        val ex = assertFailsWith<TestException> {
            deferred.await()
        }

        // then
        assertEquals("permanent failure", ex.message)
        assertEquals(2, attempts.get())
    }

    @Test
    fun `respects concurrency limit`() = runTest {
        // given
        val concurrencyLimit = 2
        val exec = CacheLoadExecutor(concurrencyLimit = concurrencyLimit)
        val current = AtomicInteger(0)
        val maxObserved = AtomicInteger(0)

        val jobs = (1..5).map {
            async {
                exec.run {
                    val cur = current.incrementAndGet()
                    maxObserved.updateAndGet { prev -> prev.coerceAtLeast(cur) }
                    delay(100)
                    current.decrementAndGet()
                    it
                }
            }
        }

        // when
        jobs.awaitAll()

        // then
        assertEquals(concurrencyLimit, maxObserved.get())
    }
}
