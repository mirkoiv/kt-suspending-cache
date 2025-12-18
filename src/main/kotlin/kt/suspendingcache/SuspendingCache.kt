package kt.suspendingcache

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Clock
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

@Suppress("UNCHECKED_CAST")
class SuspendingCache(
    private val clock: Clock = Clock.systemDefaultZone(),
    private val ttl: Duration = Duration.INFINITE,
    private val executor: CacheLoadExecutor = CacheLoadExecutor(),
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val maxSize: Int = 1000,
    private val refreshThrottle: Long = 300L,
) {

    private val scope = CoroutineScope(
        SupervisorJob() + ioDispatcher + CoroutineName("suspending-cache")
    )

    private val data = mutableMapOf<Any, CacheEntry>()

    private val mutex = Mutex()

    private val mutexPerKey = mutableMapOf<Any, Mutex>()

    suspend fun <K : Any, V : Any> get(key: K, ttl: Duration, loader: suspend () -> V): V? {
        val entry = data[key] ?: withLockFor(key) {
            addEntry(key = key, loader = loader, ttl = ttl)
        }
        return entry!!.value() as V?
    }

    suspend fun <K : Any, V : Any> get(key: K, loader: suspend () -> V): V? = get(key, ttl, loader)

    @Throws(CacheNotFoundException::class)
    suspend fun <K : Any, V : Any> get(key: K): V? {
        val entry = data[key] ?: throw CacheNotFoundException("$key")
        return entry.value() as V?
    }

    suspend fun <K : Any, V : Any> put(key: K, ttl: Duration, loader: suspend () -> V) {
        withLockFor(key) {
            data[key]?.invalidate()
            addEntry(key = key, loader = loader, ttl = ttl)
        }
    }

    suspend fun <K : Any, V : Any> put(key: K, loader: suspend () -> V) = put(key, ttl, loader)

    suspend fun <K : Any> invalidate(key: K) {
        withLockFor(key) {
            if (data.containsKey(key)) {
                data[key]!!.invalidate()
            }
        }
    }

    suspend fun <K : Any> remove(key: K) {
        withLockFor(key) {
            if (data.containsKey(key)) {
                removeEntry(key)
            }
        }
    }

    @Throws(CacheNotFoundException::class)
    suspend fun <K : Any, V : Any> refresh(key: K): V? {
        logger.debug { "Refreshing cache for $key" }
        val entry = data[key] ?: throw CacheNotFoundException("$key")
        return entry.refresh() as V?
    }

    suspend fun <K : Any> exists(key: K): Boolean = mutex.withLock {
        data.containsKey(key)
    }

    suspend fun clear() {
        scope.cancel()
        mutex.withLock {
            data.forEach { it.value.invalidate() }
            data.clear()
            mutexPerKey.clear()
        }
    }

    private suspend fun <K: Any, V: Any?> withLockFor(key: K, block: suspend () -> V): V? {
        return mutex.withLock {
            mutexPerKey.getOrPut(key) { Mutex() }
        }.withLock {
            block()
        }
    }

    private fun enforceMaxSize() {
        if (data.size >= maxSize) {
            val expired = data.entries.firstOrNull { it.value.isExpired() }?.key
            expired?.let {
                removeEntry(it)
                return
            }
            val lruKey = data.minByOrNull { it.value.lastUsedAt }?.key
            lruKey?.let {
                removeEntry(it)
            }
        }
    }

    private fun <K : Any> addEntry(key: K, loader: suspend () -> Any, ttl: Duration): CacheEntry {
        enforceMaxSize()
        data[key] = CacheEntry(
            key = key,
            scope = scope,
            loader = loader,
            executor = executor,
            ttl = ttl,
            clock = clock,
            refreshThrottle = refreshThrottle,
        )
        return data[key]!!
    }

    private fun <K : Any> removeEntry(key: K) {
        val entry = data.remove(key)
        entry?.invalidate()
        mutexPerKey.remove(key)
    }

    private class CacheEntry(
        val key: Any,
        val loader: suspend () -> Any,
        private val scope: CoroutineScope,
        private val executor: CacheLoadExecutor,
        private val ttl: Duration = Duration.INFINITE,
        private val clock: Clock = Clock.systemDefaultZone(),
        private val refreshThrottle: Long = 300L,
    ) {
        private var _lastUsedAt = 0L
        val lastUsedAt: Long get() = _lastUsedAt

        private var value: Any? = null

        private var refreshedAt = 0L

        private var expiresAt = 0L

        private var deferred: AwaiterCounterDeferred<Any?>? = null

        private val mutex = Mutex()

        init {
            logger.debug { "[$key] initializing cache entry" }
            updateTimestamps()
        }

        suspend fun value(): Any? {
            _lastUsedAt = clock.millis()

            invalidateIfExpired()

            return value ?: loadValue()
        }

        private suspend fun invalidateIfExpired() {
            if (value != null && isExpired()) {
                mutex.withLock {
                    if (value != null && isExpired()) {
                        logger.debug { "[$key] cache expired" }
                        invalidate()
                    }
                }
            }
        }

        private suspend fun loadValue(): Any? {
            logger.debug { "[$key] loadValue" }
            value?.let { return it }
            deferred?.let { return it.awaitOrNull() }

            mutex.withLock { logger.debug { "[$key] loadValue lock" }
                value?.let { return it }
                deferred?.let { return it.awaitOrNull() }

                logger.debug { "[$key] loadValue newDeferred" }
                val newDeferred = scope.async(start = CoroutineStart.LAZY) {
                    try {
                        logger.debug { "[$key] loading started" }
                        executor.run { loader() }
                    } catch (e: CancellationException) {
                        logger.debug { "[$key] loading cancelled" }
                        throw e
                    } catch (e: Exception) {
                        logger.error(e) { "[$key] loading error" }
                        null
                    }
                }
                val awaiterCounterDeferred = AwaiterCounterDeferred(newDeferred)
                deferred = awaiterCounterDeferred
                value = awaiterCounterDeferred.awaitOrNull()?.let {
                    updateTimestamps()
                    it
                }
                logger.debug { "[$key] loadValue loaded $value" }
                deferred = null
                return value
            }
        }

        fun invalidate() {
            value = null
            deferred?.cancel()
            deferred = null
        }

        suspend fun refresh(): Any? {
            if (refreshedAt == 0L || refreshedAt + refreshThrottle < clock.millis()) {
                refreshedAt = clock.millis()
                invalidate()
            }
            return value()
        }

        fun isExpired(): Boolean = expiresAt > 0L && expiresAt < clock.millis()

        private fun updateTimestamps() {
            refreshedAt = clock.millis()
            expiresAt = if (ttl != Duration.INFINITE) clock.millis() + ttl.inWholeMilliseconds else 0
        }
    }

    internal class AwaiterCounterDeferred<T>(private val deferred: Deferred<T>) {

        private val counter = AtomicInteger(0)
        val awaiters get() = counter.get()

        suspend fun await(): T {
            logger.debug { "awaiting value ..." }
            counter.incrementAndGet()
            val callerJob = currentCoroutineContext()[Job]!!
            callerJob.invokeOnCompletion { cause ->
                if (cause is CancellationException) {
                    if (counter.get() == 1 && deferred.isActive) {
                        deferred.cancel()
                    }
                }
                counter.decrementAndGet()
            }
            return deferred.await()
        }

        fun cancel() {
            logger.debug { "cancelling loader ..." }
            deferred.cancel()
        }
    }
}

private suspend fun <T> SuspendingCache.AwaiterCounterDeferred<T>.awaitOrNull(): T? {
    return try {
        logger.debug { "awaiting ..." }
        this.await()
    } catch (e: Exception) {
        logger.error(e) { "awaiting failed" }
        null
    }
}

class CacheNotFoundException(key: String) :
    RuntimeException(
        "Cache for key '$key' not found. Initialize the cache first using a loader."
    )
