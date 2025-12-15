package kt.suspendingcache

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.test.Test
import kotlin.test.assertEquals


class AwaiterCounterDeferredTest {

    @Test
    fun `await delivers result across different scopes`() = runTest {
        // given
        val loaderScope = CoroutineScope(SupervisorJob() + this.coroutineContext[CoroutineDispatcher]!!)
        val deferred = loaderScope.async {
            delay(100)
            "value"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        // when
        val result = async { cancellable.await() }.await()

        // then
        assertEquals("value", result)
        assertTrue(deferred.isCompleted)
    }

    @Test
    fun `the awaiters counter increments with every new awaiter`() = runTest {
        // given
        val dispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val loaderScope = CoroutineScope(SupervisorJob() + dispatcher)
        val deferred = loaderScope.async {
            delay(1000)
            "done"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        // when/then
        CoroutineScope(Job() + dispatcher).launch { cancellable.await() }
        advanceTimeBy(100)
        assertEquals(1, cancellable.awaiters)

        // and when/then
        CoroutineScope(Job() + dispatcher).launch { cancellable.await() }
        advanceTimeBy(100)
        assertEquals(2, cancellable.awaiters)

        // and when/then
        CoroutineScope(Job() + dispatcher).launch { cancellable.await() }
        advanceTimeBy(100)
        assertEquals(3, cancellable.awaiters)
    }

    @Test
    fun `the awaiters counter decrements when awaiter finishes`() = runTest {
        // given
        val dispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val loaderScope = CoroutineScope(SupervisorJob() + dispatcher)
        val deferred = loaderScope.async {
            delay(1000)
            "done"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        val job1 = CoroutineScope(Job() + dispatcher).launch { cancellable.await(); delay(100) }
        val job2 = CoroutineScope(Job() + dispatcher).launch { cancellable.await(); delay(200) }
        val job3 = CoroutineScope(Job() + dispatcher).launch { cancellable.await(); delay(300) }

        // when/then
        advanceTimeBy(100)
        assertEquals(3, cancellable.awaiters)

        job1.invokeOnCompletion {
            assertEquals(2, cancellable.awaiters)
        }
        job2.invokeOnCompletion {
            assertEquals(1, cancellable.awaiters)

        }
        job3.invokeOnCompletion {
            assertEquals(0, cancellable.awaiters)
        }
        joinAll(job1, job2, job3)
    }

    @Test
    fun `caller cancellation cancels deferred when there is only one awaiter`() = runTest {
        // given
        val dispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val loaderScope = CoroutineScope(SupervisorJob() + dispatcher)
        val deferred = loaderScope.async {
            delay(1000)
            "late"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        val callerScope = CoroutineScope(Job() + dispatcher)
        val job = callerScope.launch { cancellable.await() }

        advanceTimeBy(100)

        // when
        job.cancelAndJoin()

        // then
        assertTrue(deferred.isCancelled)
    }

    @Test
    fun `multiple independent callers prevent cancellation`() = runTest {
        // given
        val dispatcher = this.coroutineContext[CoroutineDispatcher]!!
        val loaderScope = CoroutineScope(SupervisorJob() + dispatcher)
        val deferred = loaderScope.async {
            delay(1000)
            "done"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        val callerScope1 = CoroutineScope(Job() + dispatcher)
        val callerScope2 = CoroutineScope(Job() + dispatcher)

        val job1 = callerScope1.launch { cancellable.await() }
        val job2 = callerScope2.launch { cancellable.await() }

        advanceTimeBy(100)

        // when
        job1.cancelAndJoin()
        // then
        assertTrue(deferred.isActive)

        // when
        job2.cancelAndJoin()
        // then
        assertTrue(deferred.isCancelled)
    }

    @Test
    fun `explicit cancel cancels deferred regardless of callers`() = runTest {
        // given
        val loaderScope = CoroutineScope(SupervisorJob() + this.coroutineContext[CoroutineDispatcher]!!)
        val deferred = loaderScope.async {
            delay(1000)
            "never"
        }
        val cancellable = SuspendingCache.AwaiterCounterDeferred(deferred)

        // when
        cancellable.cancel()

        // then
        assertTrue(deferred.isCancelled)
    }
}
