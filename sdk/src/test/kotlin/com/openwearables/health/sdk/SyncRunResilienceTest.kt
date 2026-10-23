package com.openwearables.health.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import kotlin.random.Random
import kotlin.test.*

/**
 * OW-P3 (GUT-217 #6, Codex 6C seams): the real SyncManager run — gate, grants, lease, rounds,
 * upload, retry window — against a synthetic Health Connect and synthetic HTTP. No device, no
 * network; WorkManager is replaced by a recording scheduler.
 */
class SyncRunResilienceTest {
    private val day = 86_400_000L
    private val now = System.currentTimeMillis()
    private fun recent(count: Int, spacingMs: Long = 60_000L) = (0 until count).map { now - 2 * day + it * spacingMs }

    private fun harness(tracked: List<String>, granted: Set<String>, readers: Set<String> = tracked.toSet(),
                        alias: Map<String, String> = emptyMap()) = SyncHarness(FakeProvider(tracked, granted, readers, alias))

    private fun lastAttempt(h: SyncHarness, m: SyncManager) = m.getSyncAttempts().first()
    @Suppress("UNCHECKED_CAST")
    private fun statuses(attempt: Map<String, Any?>) = attempt["typeStatuses"] as Map<String, String>

    @Test fun noGrantStopsBeforeAnyRequest() = runBlocking {
        val h = harness(listOf("steps", "heartRate", "walkingSpeed"), granted = emptySet(), readers = setOf("steps", "heartRate"))
        val m = h.manager()
        assertEquals(SyncOutcome.TERMINAL, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertEquals(0, h.http.calls.size, "no lease check, no upload")
        assertTrue(h.provider.reads.isEmpty())
        assertTrue(h.scheduler.scheduled.isEmpty(), "permission is terminal: no retry")
        val attempt = lastAttempt(h, m)
        assertEquals("no_readable_types", attempt["errorCode"])
        assertEquals(mapOf("steps" to TypeStatus.NOT_GRANTED, "heartRate" to TypeStatus.NOT_GRANTED,
            "walkingSpeed" to TypeStatus.UNSUPPORTED), statuses(attempt))
    }

    @Test fun partialGrantUploadsGrantedTypesAndLeavesTheRestPending() = runBlocking {
        val h = harness(listOf("steps", "heartRate", "sleep", "walkingSpeed"), granted = setOf("steps", "heartRate"),
            readers = setOf("steps", "heartRate", "sleep"))
        h.provider.data["steps"] = recent(30)
        h.provider.data["heartRate"] = recent(10)
        h.provider.data["sleep"] = recent(5)
        h.markInitialExportDone()
        val m = h.manager()
        assertEquals(SyncOutcome.PARTIAL, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertEquals(1, h.http.leaseChecks())
        assertEquals(1, h.http.syncPosts())
        assertFalse("sleep" in h.provider.reads, "an ungranted type is never read")
        assertFalse("walkingSpeed" in h.provider.reads, "an id with no reader is never read")
        val anchors = h.anchors()
        assertTrue("steps" in anchors && "heartRate" in anchors, anchors)
        assertFalse("sleep" in anchors, "a skipped type is never marked complete")
        val s = statuses(lastAttempt(h, m))
        assertEquals(TypeStatus.COMPLETED, s["steps"])
        assertEquals(TypeStatus.NOT_GRANTED, s["sleep"])
        assertEquals(TypeStatus.UNSUPPORTED, s["walkingSpeed"])
        assertTrue(h.scheduler.scheduled.isEmpty())
    }

    @Test fun revokedMidRunSkipsTheTypeWithoutFailingTheRound() = runBlocking {
        val h = harness(listOf("steps", "heartRate"), granted = setOf("steps", "heartRate"))
        h.provider.data["steps"] = recent(20)
        h.provider.failures["heartRate"] = { SecurityException("synthetic: READ_HEART_RATE revoked") }
        h.markInitialExportDone()
        val m = h.manager()
        assertEquals(SyncOutcome.PARTIAL, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertEquals(1, h.http.syncPosts(), "steps still upload")
        assertFalse("heartRate" in h.anchors())
        assertEquals(TypeStatus.READ_DENIED, statuses(lastAttempt(h, m))["heartRate"])
        assertTrue(h.scheduler.scheduled.isEmpty(), "a revoked grant is not retried")
    }

    @Test fun aliasesAreReadOnce() = runBlocking {
        val tracked = listOf("distanceCycling", "distanceWalkingRunning", "bloodPressureSystolic", "bloodPressure")
        val alias = mapOf("distanceCycling" to "distance", "distanceWalkingRunning" to "distance",
            "bloodPressureSystolic" to "bp", "bloodPressure" to "bp")
        val h = harness(tracked, granted = tracked.toSet(), alias = alias)
        h.provider.data["distanceWalkingRunning"] = recent(3)
        h.provider.data["bloodPressure"] = recent(2)
        h.markInitialExportDone()
        val m = h.manager()
        assertEquals(SyncOutcome.COMPLETED, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertEquals(setOf("distanceWalkingRunning", "bloodPressure"), h.provider.reads.toSet())
        val s = statuses(lastAttempt(h, m))
        assertEquals(TypeStatus.ALIAS, s["distanceCycling"])
        assertEquals(TypeStatus.ALIAS, s["bloodPressureSystolic"])
    }

    @Test fun rateLimitedUploadRetriesNoEarlierThanRetryAfterAcrossRestarts() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        h.http.sync = { 429 to mapOf("Retry-After" to "1800") }
        val m = h.manager()
        assertEquals(SyncOutcome.RETRY, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        val (delay, replace) = h.scheduler.scheduled.single()
        assertTrue(delay >= 1_800_000L, "$delay")
        assertTrue(replace)
        assertFalse("steps" in h.anchors(), "a refused upload is never accepted progress")
        assertEquals("upload_rate_limited", lastAttempt(h, m)["errorCode"])
        assertEquals(1800L, lastAttempt(h, m)["retryAfterSec"])
        assertTrue(m.hasResumableSyncSession() || h.prefs.contains(StorageKeys.KEY_RETRY_WINDOW))

        // A new process (fresh manager, same prefs): neither a periodic nor a foreground run may beat Retry-After.
        val calls = h.http.calls.size
        val reborn = h.manager()
        assertEquals(SyncOutcome.BACKED_OFF, reborn.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertEquals(SyncOutcome.BACKED_OFF, reborn.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.FOREGROUND))
        assertEquals(calls, h.http.calls.size, "inside the window nothing touches the network")

        // At the window's end the scheduled retry runs and succeeds; the window clears.
        reborn.clock = { System.currentTimeMillis() + 2 * 3_600_000L }
        h.http.sync = { 202 to emptyMap() }
        assertEquals(SyncOutcome.COMPLETED, reborn.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.RETRY))
        assertFalse(h.prefs.contains(StorageKeys.KEY_RETRY_WINDOW))
        assertEquals(0, h.scheduler.cancels.get(), "the retry never cancels itself")
    }

    @Test fun serverErrorThenOfflineBackOffExponentially() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        h.http.sync = { 503 to emptyMap() }
        val m = h.manager().apply { random = Random(5) }
        assertEquals(SyncOutcome.RETRY, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        val first = h.scheduler.scheduled.last().first
        assertTrue(first in 30_000L..60_000L, "$first")
        assertEquals("upload_transient", lastAttempt(h, m)["errorCode"])

        m.clock = { System.currentTimeMillis() + 5 * 60_000L } // past the first window
        h.http.offline = true
        assertEquals(SyncOutcome.RETRY, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.RETRY))
        val (second, replaceRunning) = h.scheduler.scheduled.last()
        assertTrue(second in 60_000L..120_000L, "$second")
        assertFalse(replaceRunning, "the running retry appends its successor")
        assertEquals("lease_unavailable", lastAttempt(h, m)["errorCode"], "offline fails at the lease check, before reading")
    }

    @Test fun refusedCredentialsStopRetrying() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        h.http.sync = { 401 to emptyMap() }
        h.http.refresh = { 401 }
        h.http.renewal = { 401 }
        val m = h.manager()
        assertEquals(SyncOutcome.TERMINAL, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertTrue(h.scheduler.scheduled.isEmpty())
        assertEquals("upload_auth", lastAttempt(h, m)["errorCode"])

        // The persisted verdict now stops the next run before any request.
        h.authState = AuthRecovery.STATE_REAUTH_REQUIRED
        val calls = h.http.calls.size
        assertEquals(SyncOutcome.TERMINAL, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertEquals(calls, h.http.calls.size)
        assertEquals("reauth_required", lastAttempt(h, m)["errorCode"])
    }

    @Test fun leaseRateLimitHonoursRetryAfterWithoutReadingHealthData() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.http.lease = { 429 to "{}" }
        val m = h.manager()
        assertEquals(SyncOutcome.RETRY, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertTrue(h.provider.reads.isEmpty())
        assertEquals(0, h.http.syncPosts())
        assertTrue(h.scheduler.scheduled.single().first >= 900_000L, "default lease Retry-After is 900 s")
        assertEquals("lease_rate_limited", lastAttempt(h, m)["errorCode"])
    }

    @Test fun anotherConnectionsLeaseWindowDoesNotBlockAPassingOwner() = runBlocking {
        val limited = harness(listOf("steps"), granted = setOf("steps"))
        limited.http.lease = { 429 to "{}" }
        assertEquals(SyncOutcome.RETRY, limited.manager().syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(3)
        h.markInitialExportDone()
        val m = h.manager()
        assertEquals(SyncOutcome.COMPLETED, m.syncNow("https://ow.synthetic.invalid", null, false))
        h.provider.data["steps"] = recent(6)
        assertEquals(SyncOutcome.COMPLETED, m.syncNow("https://ow.synthetic.invalid", null, false), "the process-wide 429 window ended with an active lease")
        assertEquals(2, h.http.leaseChecks())
    }

    @Test fun overlappingRunsAreSerialised() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        val hold = CompletableDeferred<Unit>()
        h.provider.hold = hold
        val foreground = h.manager()
        val worker = h.manager() // HealthSyncWorker builds its own manager
        val first = async(Dispatchers.IO) { foreground.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.FOREGROUND) }
        withTimeout(5_000) { h.provider.entered.await() }
        assertEquals(SyncOutcome.BUSY, worker.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertEquals(SyncOutcome.BUSY, worker.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.EXPEDITED))
        assertEquals(true, worker.getSyncStatus()["isSyncing"])
        hold.complete(Unit)
        assertEquals(SyncOutcome.COMPLETED, first.await())
        assertEquals(1, h.provider.maxActive)
        assertEquals(1, h.http.syncPosts())
    }

    @Test fun aNewConnectionWaitsForTheSupersededRunToStop() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        val hold = CompletableDeferred<Unit>()
        h.provider.hold = hold
        val old = async(Dispatchers.IO) { h.manager().syncNow("https://ow.synthetic.invalid", null, false) }
        withTimeout(5_000) { h.provider.entered.await() }
        h.owner = SecureStorage.ConnectionOwner("member-1", "gen-" + java.util.UUID.randomUUID()) // re-Connect
        h.provider.hold = null
        val fresh = async(Dispatchers.IO) { h.manager().syncNow("https://ow.synthetic.invalid", null, false) }
        hold.complete(Unit)
        assertEquals(SyncOutcome.SUPERSEDED, old.await())
        assertEquals(SyncOutcome.COMPLETED, fresh.await())
        assertEquals(1, h.http.syncPosts(), "only the new connection uploads")
    }

    @Test fun exhaustedPageSessionRollsOverInsteadOfFailing() = runBlocking {
        val h = harness(listOf("steps"), granted = setOf("steps"))
        h.provider.data["steps"] = recent(1_000) // 5 pages of 200
        h.markInitialExportDone()
        val m = h.manager().apply { nativePageBudget = 3 }
        assertEquals(SyncOutcome.COMPLETED, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertTrue(h.http.syncPosts() >= 5, "${h.http.syncPosts()}")
        assertTrue(h.logs.any { "rolled over" in it })
        assertTrue("steps" in h.anchors())
    }

    @Test fun aSessionThatCannotRollOverParksTheTypeOnly() = runBlocking {
        val h = harness(listOf("steps", "heartRate"), granted = setOf("steps", "heartRate"))
        h.provider.data["steps"] = List(1_000) { now - day } // every record at one instant: no watermark progress
        h.provider.data["heartRate"] = recent(3)
        h.markInitialExportDone()
        val m = h.manager().apply { nativePageBudget = 3 }
        assertEquals(SyncOutcome.PARTIAL, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertEquals(TypeStatus.PAGE_BUDGET_STALLED, statuses(lastAttempt(h, m))["steps"])
        assertEquals(TypeStatus.COMPLETED, statuses(lastAttempt(h, m))["heartRate"])
    }

    @Test fun transientReadFailureKeepsTheSessionThenParksARepeatOffender() = runBlocking {
        val h = harness(listOf("steps", "heartRate"), granted = setOf("steps", "heartRate"))
        h.provider.data["steps"] = recent(5)
        h.provider.failures["heartRate"] = { IllegalStateException("synthetic: Health Connect busy") }
        h.markInitialExportDone()
        val m = h.manager()
        var clock = System.currentTimeMillis()
        m.clock = { clock }
        assertEquals(SyncOutcome.RETRY, m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.PERIODIC))
        assertEquals("read_transient", lastAttempt(h, m)["errorCode"])
        assertTrue(m.hasResumableSyncSession(), "steps' completion and heartRate's place are kept")
        repeat(2) {
            clock += 8 * 3_600_000L
            m.syncNow("https://ow.synthetic.invalid", null, false, SyncTrigger.RETRY)
        }
        val attempt = lastAttempt(h, m)
        assertEquals(TypeStatus.READ_FAILED_REPEATEDLY, statuses(attempt)["heartRate"])
        assertEquals("partial", attempt["outcome"])
        assertFalse(m.hasResumableSyncSession())
    }

    @Test fun unsupportedTypeReachingTheReaderIsSkippedNotFatal() = runBlocking {
        // An old persisted 43-id list with a provider that claims a reader it does not have.
        val h = harness(listOf("steps", "walkingSpeed"), granted = setOf("steps", "walkingSpeed"), readers = setOf("steps"))
        h.provider.claimAllReadable = true
        h.provider.data["steps"] = recent(5)
        h.markInitialExportDone()
        val m = h.manager()
        assertEquals(SyncOutcome.COMPLETED, m.syncNow("https://ow.synthetic.invalid", null, false))
        assertEquals(1, h.http.syncPosts())
    }
}
