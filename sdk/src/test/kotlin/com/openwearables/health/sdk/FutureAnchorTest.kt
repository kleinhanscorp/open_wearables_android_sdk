package com.openwearables.health.sdk

import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.time.Instant
import java.time.ZoneId
import kotlin.test.*

/**
 * GUT-217 ("Android uploads ONCE a day", S23, 23 Oct): Samsung Health writes a day's steps total
 * from local midnight to the NEXT midnight. Its end became the steps anchor, so every later window
 * started after it ended and Health Connect's `TimeRangeFilter.between` threw before any upload.
 * The replay of the phone runs on the real clock; the others on a fixed one at yesterday's local
 * noon, so no result depends on the time of day.
 */
class FutureAnchorTest {
    private val hour = 3_600_000L
    private val zone = ZoneId.systemDefault()
    private val host = "https://ow.synthetic.invalid"

    private fun dayStart(ms: Long, plusDays: Long = 0) = Instant.ofEpochMilli(ms).atZone(zone).toLocalDate()
        .plusDays(plusDays).atStartOfDay(zone).toInstant().toEpochMilli()

    /** Yesterday at local noon: every timestamp below is in the past for the real clock too. */
    private val noon = dayStart(System.currentTimeMillis(), -1) + 12 * hour
    private val midnight = dayStart(noon)
    private val nextMidnight = dayStart(noon, 1)

    private fun dailyTotal(start: Long, value: Double) =
        Span("steps-total-$start", start, Instant.ofEpochMilli(start).atZone(zone).plusDays(1).toInstant().toEpochMilli(), value)

    private fun harness(tracked: List<String>) = SyncHarness(FakeProvider(tracked, tracked.toSet(), tracked.toSet()))

    @Suppress("UNCHECKED_CAST")
    private fun statuses(m: SyncManager) = m.getSyncAttempts().first()["typeStatuses"] as Map<String, String>

    @Test fun aFutureAnchorIsHealedAndNoLongerStopsEveryRun() = runBlocking {
        // The S23's state, on the real clock: the steps anchor at the next local midnight, heart rate in the past.
        val now = System.currentTimeMillis()
        val today = dailyTotal(dayStart(now), 4_000.0)
        val h = harness(listOf("steps", "heartRate"))
        h.provider.spans["steps"] = listOf(today)
        h.provider.data["heartRate"] = listOf(now - hour, now - 30 * 60_000L)
        h.storeAnchors("steps" to today.endMs, "heartRate" to now - 2 * hour)
        h.markInitialExportDone()
        val m = h.manager().apply { clock = { now } }

        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC), h.logs.joinToString("\n"))
        assertEquals(1, h.http.syncPosts())
        assertTrue(h.scheduler.scheduled.isEmpty(), "no retry")
        assertTrue(h.provider.queries.all { it.second.startMs < it.second.endMs }, "every window reaching Health Connect is valid")
        assertEquals(listOf(today.id), h.provider.served.toList(), "today's total is read from the start of the day")
        assertTrue(h.logs.any { "steps: anchor" in it && "is in the future" in it }, h.logs.joinToString("\n"))
        assertEquals(today.startMs, h.anchorOf("steps"), "committed at the open total's start, not its end")
        assertEquals(now - 30 * 60_000L, h.anchorOf("heartRate"))
    }

    @Test fun anEmptyWindowIsCompleteNotAnError() = runBlocking {
        val h = harness(listOf("steps", "heartRate"))
        h.provider.data["steps"] = listOf(noon - hour)
        h.provider.data["heartRate"] = listOf(noon - hour)
        h.storeAnchors("steps" to noon - 2 * hour, "heartRate" to noon) // heart rate's anchor IS the session's end
        h.markInitialExportDone()
        val m = h.manager().apply { clock = { noon } }

        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC))
        assertFalse("heartRate" in h.provider.reads, "an empty window never reaches Health Connect")
        assertEquals(TypeStatus.COMPLETED, statuses(m)["heartRate"])
        assertEquals(TypeStatus.COMPLETED, statuses(m)["steps"])
        assertEquals(noon, h.anchorOf("heartRate"), "an empty window leaves the anchor where it was")
        assertEquals(1, h.http.syncPosts(), "steps still upload")
    }

    @Test fun aGrowingDailyTotalIsReadAgainUntilItCloses() = runBlocking {
        val h = harness(listOf("steps"))
        val yesterday = dailyTotal(dayStart(noon, -1), 9_000.0)
        val today = dailyTotal(midnight, 3_000.0)
        h.provider.spans["steps"] = listOf(yesterday, today)
        var clock = noon
        val m = h.manager().apply { this.clock = { clock } }

        // First Connect: the full export reads both days and commits no anchor past the session.
        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false))
        assertEquals(midnight, h.anchorOf("steps"), "never the open total's end (the next midnight)")

        // 15 minutes later the total has grown: it is read again (OW keeps the newer value), yesterday's is not.
        h.provider.served.clear()
        clock += 15 * 60_000L
        h.provider.spans["steps"] = listOf(yesterday, today.copy(value = 3_400.0))
        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC))
        assertEquals(listOf(today.id), h.provider.served.toList())
        assertEquals(midnight, h.anchorOf("steps"), "held at the open total's start")

        // After midnight the closed total is read one last time beside the new open one; the anchor moves on.
        h.provider.served.clear()
        clock = nextMidnight + hour
        val tomorrow = dailyTotal(nextMidnight, 200.0)
        h.provider.spans["steps"] = listOf(yesterday, today.copy(value = 11_000.0), tomorrow)
        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC))
        assertEquals(listOf(today.id, tomorrow.id), h.provider.served.toList())
        assertEquals(nextMidnight, h.anchorOf("steps"))
        assertEquals(3, h.http.syncPosts())
    }

    @Test fun theSamsungPathCommitsNoAnchorPastTheSessionEither() = runBlocking {
        val h = harness(listOf("steps"))
        h.provider.nativePages = false
        val closed = Span("steps-closed", noon - 2 * hour, noon - hour)
        val open = Span("steps-open", noon - 30 * 60_000L, noon + 2 * hour)
        h.provider.spans["steps"] = listOf(closed, open)
        h.storeAnchors("steps" to nextMidnight)
        h.markInitialExportDone()
        val m = h.manager().apply { clock = { noon } }

        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC))
        assertEquals(listOf(closed.id, open.id), h.provider.served.toList(), "healed to the start of the day")
        assertEquals(noon, h.anchorOf("steps"), "the open record's end is clamped to the session's end")

        // The same instant again: the cursor sits at the session's end, so nothing is read and nothing fails.
        val reads = h.provider.reads.size
        assertEquals(SyncOutcome.COMPLETED, m.syncNow(host, null, false, SyncTrigger.PERIODIC))
        assertEquals(reads, h.provider.reads.size)
    }
}
