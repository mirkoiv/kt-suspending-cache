package kt.suspendingcache.utils

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class FakeClock(
    private var instant: Instant,
    private val zone: ZoneId = ZoneId.systemDefault(),
) : Clock() {

    fun adjust(time: Duration) {
        instant += time.toJavaDuration()
    }

    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock {
        return if (this.zone.id == zone.id) {
            this
        } else {
            FakeClock(instant, zone)
        }
    }

    override fun instant(): Instant {
        return instant
    }
}
