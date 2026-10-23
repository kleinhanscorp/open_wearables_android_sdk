package com.openwearables.health.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import kotlin.random.Random
import kotlin.test.*

/** OW-P3 (GUT-217 #6): the run's rules without Android — readable types, failure classes, backoff, rollover, gate, attempt log. */
class SyncPolicyTest {
    private val readers = mapOf(
        "steps" to "Steps", "heartRate" to "HeartRate", "sleep" to "Sleep",
        "distanceWalkingRunning" to "Distance", "distanceCycling" to "Distance",
        "bloodPressure" to "BloodPressure", "bloodPressureSystolic" to "BloodPressure", "bloodPressureDiastolic" to "BloodPressure",
        "dietaryWater" to "Hydration", "water" to "Hydration"
    )
    private fun resolve(tracked: List<String>, granted: Set<String>) =
        SyncPolicy.resolveReadable(tracked, { readers[it] }, { readers[it]?.let { r -> "read:$r" } }, granted.map { "read:$it" }.toSet())

    @Test fun noGrantLeavesNothingReadable() {
        val result = resolve(listOf("steps", "heartRate"), emptySet())
        assertTrue(result.readable.isEmpty())
        assertEquals(mapOf("steps" to TypeStatus.NOT_GRANTED, "heartRate" to TypeStatus.NOT_GRANTED), result.statuses)
    }

    @Test fun partialGrantReadsOnlyGrantedTypes() {
        val result = resolve(listOf("steps", "heartRate", "sleep"), setOf("Steps", "Sleep"))
        assertEquals(listOf("steps", "sleep"), result.readable)
        assertEquals(TypeStatus.NOT_GRANTED, result.statuses["heartRate"])
    }

    @Test fun revokedTypeDropsOutOnTheNextRun() {
        val tracked = listOf("steps", "heartRate")
        assertEquals(tracked, resolve(tracked, setOf("Steps", "HeartRate")).readable)
        assertEquals(listOf("steps"), resolve(tracked, setOf("Steps")).readable)
    }

    @Test fun unsupportedIdsNeverReachAReader() {
        val result = resolve(listOf("steps", "walkingSpeed", "mindfulSession"), setOf("Steps"))
        assertEquals(listOf("steps"), result.readable)
        assertEquals(TypeStatus.UNSUPPORTED, result.statuses["walkingSpeed"])
        assertEquals(TypeStatus.UNSUPPORTED, result.statuses["mindfulSession"])
    }

    @Test fun aliasIdsAreReadOnceThroughThePreferredId() {
        val all = listOf("distanceCycling", "distanceWalkingRunning", "bloodPressureSystolic", "bloodPressureDiastolic",
            "bloodPressure", "water", "dietaryWater")
        val result = resolve(all, setOf("Distance", "BloodPressure", "Hydration"))
        assertEquals(listOf("distanceWalkingRunning", "bloodPressure", "dietaryWater"), result.readable)
        for (alias in listOf("distanceCycling", "bloodPressureSystolic", "bloodPressureDiastolic", "water")) {
            assertEquals(TypeStatus.ALIAS, result.statuses[alias], alias)
        }
        // Only the alias tracked: it reads, rather than silently dropping the records.
        assertEquals(listOf("distanceCycling"), resolve(listOf("distanceCycling"), setOf("Distance")).readable)
    }

    @Test fun shortfallExcludesProviderShape() {
        assertFalse(SyncPolicy.isShortfall(TypeStatus.UNSUPPORTED))
        assertFalse(SyncPolicy.isShortfall(TypeStatus.ALIAS))
        assertFalse(SyncPolicy.isShortfall(TypeStatus.COMPLETED))
        for (s in listOf(TypeStatus.NOT_GRANTED, TypeStatus.READ_DENIED, TypeStatus.READ_FAILED_REPEATEDLY,
                TypeStatus.PAGE_BUDGET_STALLED, TypeStatus.PENDING)) assertTrue(SyncPolicy.isShortfall(s), s)
    }

    @Test fun failureClasses() {
        for (code in listOf(null, 408, 425, 429, 500, 502, 503, 504)) assertEquals(SyncPolicy.FailureKind.TRANSIENT, SyncPolicy.classifyHttp(code), "$code")
        for (code in listOf(401, 403)) assertEquals(SyncPolicy.FailureKind.AUTH, SyncPolicy.classifyHttp(code))
        for (code in listOf(400, 404, 413, 422)) assertEquals(SyncPolicy.FailureKind.REJECTED, SyncPolicy.classifyHttp(code))
    }

    @Test fun retryAfterParsesSecondsAndDates() {
        val now = 1_790_000_000_000L
        assertEquals(900L, SyncPolicy.parseRetryAfterSeconds(" 900 ", now))
        val date = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
            .format(java.time.Instant.ofEpochMilli(now + 120_000).atZone(java.time.ZoneOffset.UTC))
        assertEquals(120L, SyncPolicy.parseRetryAfterSeconds(date, now))
        assertNull(SyncPolicy.parseRetryAfterSeconds("soon", now))
        assertNull(SyncPolicy.parseRetryAfterSeconds(null, now))
    }

    @Test fun backoffIsExponentialJitteredCappedAndNeverBeforeRetryAfter() {
        val random = Random(17)
        repeat(50) {
            val first = SyncPolicy.backoffDelayMs(1, null, random)
            assertTrue(first in 30_000L..60_000L, "$first")
            val third = SyncPolicy.backoffDelayMs(3, null, random)
            assertTrue(third in 120_000L..240_000L, "$third")
            val capped = SyncPolicy.backoffDelayMs(30, null, random)
            assertTrue(capped in SyncPolicy.BACKOFF_CAP_MS / 2..SyncPolicy.BACKOFF_CAP_MS, "$capped")
        }
        assertTrue(SyncPolicy.backoffDelayMs(1, 1_800, random) >= 1_800_000L)
        assertEquals(SyncPolicy.RETRY_AFTER_CAP_MS, SyncPolicy.backoffDelayMs(1, 999_999, random))
    }

    @Test fun rolloverMovesForwardOrStops() {
        assertEquals(500L to 1_000L, SyncPolicy.rolloverBounds(0, 1_000, true, 500, null))
        assertNull(SyncPolicy.rolloverBounds(500, 1_000, true, 500, null), "no accepted progress: stalled")
        assertNull(SyncPolicy.rolloverBounds(0, 1_000, true, null, null))
        assertEquals(0L to 301L, SyncPolicy.rolloverBounds(0, 1_000, false, null, 300))
        assertNull(SyncPolicy.rolloverBounds(0, 301, false, null, 300), "oldest did not move: stalled")
    }

    @Test fun anOpenRecordHoldsTheAnchorAtItsStart() {
        // GUT-217: spans are (start, end); the query ends at 1 000.
        assertEquals(500L, SyncPolicy.openStartMs(listOf(100L to 200L, 700L to 1_500L, 500L to 2_000L), 1_000L))
        assertNull(SyncPolicy.openStartMs(listOf(100L to 200L, 300L to 1_000L), 1_000L), "ending at the query's end is closed")
        val end = 10 * SyncPolicy.OPEN_RECORD_HOLD_MAX_MS
        assertNull(SyncPolicy.openStartMs(listOf(end - SyncPolicy.OPEN_RECORD_HOLD_MAX_MS - 1 to Long.MAX_VALUE), end),
            "a record open for days (a far-future end) never holds the anchor")
        assertEquals(500L, SyncPolicy.committableAnchor(2_000L, 1_000L, 500L))
        assertEquals(1_000L, SyncPolicy.committableAnchor(2_000L, 1_000L, null), "never past the query's end")
        assertEquals(800L, SyncPolicy.committableAnchor(800L, 1_000L, null))
        assertNull(SyncPolicy.committableAnchor(null, 1_000L, null), "nothing read: nothing to commit")
    }

    @Test fun aFutureAnchorHealsToTheStartOfTheLocalDay() {
        // The S23 on 23 Oct: 21:40 SAST, steps anchor at the next local midnight.
        val zone = java.time.ZoneId.of("Africa/Johannesburg")
        fun at(day: Int, hour: Int, minute: Int = 0) =
            java.time.ZonedDateTime.of(2026, 10, day, hour, minute, 0, 0, zone).toInstant().toEpochMilli()
        val now = at(23, 21, 40)
        assertEquals(at(23, 0), SyncPolicy.healedAnchor(at(24, 0), now, zone))
        assertEquals(now - 1, SyncPolicy.healedAnchor(now - 1, now, zone), "a past anchor is kept")
        assertEquals(now, SyncPolicy.healedAnchor(now, now, zone))
    }

    @Test fun backoffWindowSurvivesRestartAndIsOwnerScoped() {
        val prefs = FakePrefs()
        val now = 1_790_000_000_000L
        val window = RetryBackoffStore(prefs) { Random(1) }.recordFailure("user.a", 900, now)
        assertEquals(1, window.failures)
        assertTrue(window.floorUntil >= now + 900_000)

        val reborn = RetryBackoffStore(prefs) { Random(2) } // a new process reading the same prefs
        assertNotNull(reborn.remainingMs("user.a", now + 1_000, foreground = false))
        assertNotNull(reborn.remainingMs("user.a", now + 1_000, foreground = true), "Retry-After binds foreground too")
        assertNull(reborn.remainingMs("user.b", now + 1_000, foreground = false), "another member inherits nothing")
        assertNull(reborn.remainingMs("user.a", now + 900_000 + SyncPolicy.BACKOFF_CAP_MS, foreground = false))
        assertEquals(2, reborn.recordFailure("user.a", null, now + 1_000_000).failures)
        reborn.clear()
        assertNull(reborn.read("user.a"))
    }

    @Test fun softWindowBindsBackgroundOnlyAndStreakResets() {
        val prefs = FakePrefs()
        val store = RetryBackoffStore(prefs) { Random(3) }
        val now = 1_790_000_000_000L
        store.recordFailure("user.a", null, now)
        assertNotNull(store.remainingMs("user.a", now + 1_000, foreground = false))
        assertNull(store.remainingMs("user.a", now + 1_000, foreground = true), "the member opening the app may try now")
        val later = now + SyncPolicy.BACKOFF_STREAK_RESET_MS + 3_600_000
        assertEquals(1, store.recordFailure("user.a", null, later).failures, "a day later is a fresh streak")
    }

    @Test fun gateCoalescesTheSameOwnerAndQueuesANewOne() = runBlocking {
        val a = SecureStorage.ConnectionOwner("member", "gen-a")
        val b = SecureStorage.ConnectionOwner("member", "gen-b")
        val inside = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val first = async(Dispatchers.IO) { SyncRunGate.exclusive(a, busy = { "busy" }) { inside.complete(Unit); release.await(); "first" } }
        withTimeout(5_000) { inside.await() }
        assertTrue(SyncRunGate.isRunning)
        assertEquals("busy", SyncRunGate.exclusive(a, busy = { "busy" }) { "second" })
        assertEquals("busy", SyncRunGate.exclusive(b, waitMs = 50, busy = { "busy" }) { "timed" })
        val queued = async(Dispatchers.IO) { SyncRunGate.exclusive(b, waitMs = 5_000, busy = { "busy" }) { "new owner" } }
        release.complete(Unit)
        assertEquals("first", first.await())
        assertEquals("new owner", queued.await())
        assertFalse(SyncRunGate.isRunning)
    }

    @Test fun attemptLogIsBoundedNewestFirstAndOwnerScoped() {
        val prefs = FakePrefs()
        val log = SyncAttemptLog(prefs, max = 3)
        for (i in 1..5) log.save(SyncAttempt("a$i", "user.a", SyncTrigger.PERIODIC, i.toLong()))
        log.save(SyncAttempt("b1", "user.b", SyncTrigger.PERIODIC, 9))
        assertEquals(listOf("a5", "a4"), SyncAttemptLog(prefs, max = 3).list("user.a").map { it.id })
        val updated = SyncAttempt("a5", "user.a", SyncTrigger.PERIODIC, 5).apply { outcome = "completed"; typeStatuses["steps"] = TypeStatus.COMPLETED }
        log.save(updated)
        val top = log.list("user.a").first()
        assertEquals("completed", top.toMap()["outcome"])
        assertEquals(mapOf("steps" to TypeStatus.COMPLETED), top.toMap()["typeStatuses"])
        log.clear()
        assertTrue(log.list("user.a").isEmpty())
    }
}
