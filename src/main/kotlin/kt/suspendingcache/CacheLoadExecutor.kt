package kt.suspendingcache

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.pow

private val logger = KotlinLogging.logger {}

class CacheLoadExecutor(
    concurrencyLimit: Int = 10,
    private val maxAttempts: Int = 3,
    private val initialDelay: Long = 100,
    private val multiplier: Double = 2.0,
    private val maxDelay: Long = 10_000,
) {
    private val semaphore: Semaphore = Semaphore(concurrencyLimit)

    suspend fun <T> run(loader: suspend () -> T): T {
        return retry {
            limitConcurrency {
                loader()
            }
        }
    }

    private suspend fun <T> limitConcurrency(block: suspend () -> T): T {
        return semaphore.withPermit {
            block()
        }
    }

    private suspend fun <T> retry(block: suspend () -> T): T {
        var delay = initialDelay
        var attempt = 1
        repeat(maxAttempts - 1) {
            try {
                return block()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.debug(e) { "Attempt #$attempt failed, retrying after $delay ms" }
                delay(delay)
                attempt++
                delay = (initialDelay * multiplier.pow(attempt)).toLong()
                    .coerceAtMost(maxDelay)
            }
        }
        return block()
    }
}
