package com.openwearables.health.sdk

import android.content.Context
import android.content.SharedPreferences
import androidx.work.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * Gutsy fork (OW-P3, GUT-217 #6) — how a transient failure becomes a later attempt.
 *
 * A failed upload used to return normally, so `HealthSyncWorker` reported success and nothing came
 * back until the next 15-minute period; a lease 429 lived in memory only. Now a [SyncOutcome.RETRY]
 * persists a backoff window ([RetryBackoffStore]) and schedules ONE WorkManager attempt at its end
 * ([RetryScheduler]); every other run inside the window stops before any network call.
 */
interface RetryScheduler {
    /** [replaceRunning] = false when the caller IS the scheduled retry: append instead of cancelling itself. */
    fun schedule(delayMs: Long, host: String, customSyncUrl: String?, replaceRunning: Boolean)
    fun cancel()
}

internal class WorkManagerRetryScheduler(private val context: Context) : RetryScheduler {
    override fun schedule(delayMs: Long, host: String, customSyncUrl: String?, replaceRunning: Boolean) {
        val work = OneTimeWorkRequestBuilder<HealthSyncWorker>()
            .setInitialDelay(delayMs.coerceAtLeast(0L), TimeUnit.MILLISECONDS)
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
            .setInputData(workDataOf(
                HealthSyncWorker.KEY_HOST to host,
                HealthSyncWorker.KEY_CUSTOM_SYNC_URL to customSyncUrl,
                HealthSyncWorker.KEY_TRIGGER to SyncTrigger.RETRY
            ))
            .build()
        // REPLACE only meets a retry still waiting out its delay (GUT-217, checked 24 Oct): a retry run
        // holds SyncRunGate while it reads and uploads, so no other run reaches settle() meanwhile.
        val policy = if (replaceRunning) ExistingWorkPolicy.REPLACE else ExistingWorkPolicy.APPEND_OR_REPLACE
        WorkManager.getInstance(context).enqueueUniqueWork(SyncDefaults.WORK_NAME_RETRY, policy, work)
    }

    override fun cancel() {
        WorkManager.getInstance(context).cancelUniqueWork(SyncDefaults.WORK_NAME_RETRY)
    }
}

/**
 * The persisted retry window, scoped to the member (user key) whose run failed. [nextAttemptAt] is
 * the jittered exponential point background runs wait for; [floorUntil] is the server's Retry-After,
 * which no trigger may beat. Survives process death; a sign-in or sign-out clears it.
 */
internal class RetryBackoffStore(private val prefs: SharedPreferences, private val random: () -> Random = { Random.Default }) {
    @Serializable
    data class Window(val ownerKey: String, val failures: Int, val nextAttemptAt: Long, val floorUntil: Long)

    private val json = Json { ignoreUnknownKeys = true }

    fun read(ownerKey: String): Window? = try {
        prefs.getString(StorageKeys.KEY_RETRY_WINDOW, null)
            ?.let { json.decodeFromString<Window>(it) }
            ?.takeIf { it.ownerKey == ownerKey }
    } catch (_: Exception) {
        null
    }

    fun recordFailure(ownerKey: String, retryAfterSec: Long?, nowMs: Long): Window {
        val previous = read(ownerKey)?.takeIf { nowMs - it.nextAttemptAt < SyncPolicy.BACKOFF_STREAK_RESET_MS }
        val failures = (previous?.failures ?: 0) + 1
        val delay = SyncPolicy.backoffDelayMs(failures, retryAfterSec, random())
        val floor = retryAfterSec?.let { nowMs + (it * 1000L).coerceIn(0L, SyncPolicy.RETRY_AFTER_CAP_MS) } ?: 0L
        val window = Window(ownerKey, failures, nowMs + delay, maxOf(floor, previous?.floorUntil ?: 0L))
        prefs.edit().putString(StorageKeys.KEY_RETRY_WINDOW, json.encodeToString(window)).commit()
        return window
    }

    /** Milliseconds this run must still wait, or `null` when it may start now. */
    fun remainingMs(ownerKey: String, nowMs: Long, foreground: Boolean): Long? {
        val window = read(ownerKey) ?: return null
        val until = if (foreground) window.floorUntil else maxOf(window.nextAttemptAt, window.floorUntil)
        // A scheduled retry may be woken a moment early; that is its window, not a new one.
        return (until - nowMs).takeIf { it > SyncDefaults.RETRY_EARLY_TOLERANCE_MS }
    }

    fun clear() {
        prefs.edit().remove(StorageKeys.KEY_RETRY_WINDOW).commit()
    }
}

/**
 * ONE sync run per process. The SDK's own manager (foreground, resume, reset) and every worker
 * (periodic, expedited, retry) build separate [SyncManager]s, so their per-instance flags never
 * serialised them. A second run for the same owner+generation coalesces into the running one;
 * a run for a newer connection waits briefly for the superseded run to hit its fence and stop.
 */
internal object SyncRunGate {
    private val mutex = Mutex()
    @Volatile private var holder: SecureStorage.ConnectionOwner? = null

    val isRunning: Boolean get() = mutex.isLocked

    suspend fun <T> exclusive(
        owner: SecureStorage.ConnectionOwner,
        waitMs: Long = SyncDefaults.SUPERSEDED_RUN_WAIT_MS,
        busy: () -> T,
        block: suspend () -> T
    ): T {
        if (!mutex.tryLock()) {
            if (holder == owner) return busy()
            if (withTimeoutOrNull(waitMs) { mutex.lock() } == null) return busy()
        }
        holder = owner
        try {
            return block()
        } finally {
            holder = null
            mutex.unlock()
        }
    }
}
