package com.openwearables.health.sdk

import android.content.Context
import androidx.work.WorkManager
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.*

/**
 * GUT-217 ("the 30-second upload loop", S23, 24 Oct). Every `configure()` re-ran the auto-restore,
 * so the app's 30-second retry of a health read became an upload every 30 s for 4.5 minutes. And the
 * expedited request was enqueued with REPLACE, so a second trigger (an activity pause, the JS
 * catch-up, `configure()`) cancelled the run already uploading (13:07:44, JobCancellationException)
 * and its replacement came back BUSY. The real SDK and SyncManager run here; storage is mocked and
 * WorkManager is [FakeWorkManager], which applies the unique-work policies.
 */
class SyncTriggerTest {
    private val host = "https://ow.synthetic.invalid"
    private val periodic = SyncDefaults.WORK_NAME_PERIODIC
    private val expedited = SyncDefaults.WORK_NAME_EXPEDITED
    private val wm = FakeWorkManager()
    @Volatile private var syncActive = true
    private lateinit var sdk: OpenWearablesHealthSDK

    @Before fun setUp() {
        mockkStatic(WorkManager::class)
        every { WorkManager.getInstance(any()) } returns wm.mock
        mockkConstructor(SecureStorage::class)
        every { anyConstructed<SecureStorage>().clearIfReinstalled() } just Runs
        every { anyConstructed<SecureStorage>().saveHost(any()) } just Runs
        every { anyConstructed<SecureStorage>().getCustomSyncUrl() } returns null
        every { anyConstructed<SecureStorage>().getTrackedTypes() } returns listOf("steps", "heartRate")
        every { anyConstructed<SecureStorage>().getProvider() } returns ProviderIds.GOOGLE
        every { anyConstructed<SecureStorage>().isSyncActive() } answers { syncActive }
        every { anyConstructed<SecureStorage>().setSyncActive(any()) } answers { syncActive = firstArg() }
        every { anyConstructed<SecureStorage>().hasSession() } returns true
        every { anyConstructed<SecureStorage>().getUserId() } returns "member-1"
        every { anyConstructed<SecureStorage>().hasAuth } returns true
        val context = mockk<Context>(relaxed = true).also { every { it.applicationContext } returns it }
        sdk = OpenWearablesHealthSDK.initializeForTesting(context, TestDispatchers,
            healthConnectFactory = { _, _, _, _ -> mockk(relaxed = true) })
    }

    @After fun tearDown() {
        unmockkConstructor(SecureStorage::class)
        unmockkStatic(WorkManager::class)
    }

    @Test fun repeatedConfigureSchedulesBackgroundSyncOnce() {
        // Nine retries 30 s apart after the first call: the HealthContext loop of 24 Oct.
        repeat(10) { assertTrue(sdk.configure(host), "sync was active") }
        assertEquals(1, wm.enqueues(expedited), wm.calls.toString())
        assertEquals(1, wm.enqueues(periodic), wm.calls.toString())
    }

    @Test fun configureForAnotherEndpointRestoresAgain() {
        sdk.configure(host)
        sdk.configure(host)
        sdk.configure("https://other.synthetic.invalid")
        assertEquals(2, wm.enqueues(periodic), wm.calls.toString())
    }

    @Test fun configureAfterAnExplicitStartDoesNotScheduleAgain() = runBlocking {
        syncActive = false
        sdk.configure(host) // the first launch: nothing to restore
        sdk.startBackgroundSync() // the member connects
        sdk.configure(host) // a JS reload in the same process
        assertEquals(1, wm.enqueues(periodic), wm.calls.toString())
    }

    @Test fun aStoppedSyncIsRestoredByTheNextStart() = runBlocking {
        sdk.configure(host)
        sdk.stopBackgroundSync()
        assertTrue(wm.pending(periodic).isEmpty())
        sdk.startBackgroundSync()
        sdk.configure(host)
        assertEquals(1, wm.pending(periodic).size)
        assertEquals(2, wm.enqueues(periodic), wm.calls.toString())
    }

    @Test fun aSecondExpeditedTriggerNeverCancelsTheRunInFlight() = runBlocking {
        sdk.configure(host) // cold start: the restore enqueues the expedited run
        val run = wm.start(expedited) // WorkManager starts it; it is uploading
        val period = wm.start(periodic)
        sdk.onBackground() // an activity pause (13:07:44)
        sdk.startBackgroundSync() // the JS catch-up
        sdk.ensureSyncManager().scheduleExpeditedSync(host, null)
        assertEquals(emptyList(), wm.cancelledWhileRunning().map { it.name }, wm.calls.toString())
        assertEquals(FakeWorkManager.State.RUNNING, run.state)
        assertEquals(FakeWorkManager.State.RUNNING, period.state)
        assertEquals(listOf(run), wm.pending(expedited), "no second upload queued behind it")
    }

    @Test fun aTriggerAfterTheRunFinishedSchedulesTheNextRun() {
        sdk.configure(host)
        wm.start(expedited)
        wm.finish(expedited)
        sdk.onBackground()
        assertEquals(1, wm.pending(expedited).size, wm.calls.toString())
        assertEquals(FakeWorkManager.State.ENQUEUED, wm.pending(expedited).single().state)
    }
}
