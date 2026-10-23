package com.openwearables.health.sdk

import android.content.Context
import android.content.SharedPreferences
import androidx.work.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.cancellation.CancellationException
import kotlin.random.Random

@Serializable
data class TypeSyncProgress(
    val typeIdentifier: String,
    var sentCount: Int = 0,
    var isComplete: Boolean = false,
    var pendingAnchorTimestamp: Long? = null,
    var pendingOlderThan: Long? = null,
    var nativePageToken: String? = null,
    var queryStartMs: Long? = null,
    var queryEndMs: Long? = null,
    var nativePageSize: Int? = null,
    var nativePageCount: Int = 0,
    var nativeTokenHashes: MutableSet<String> = mutableSetOf(),
    // Gutsy fork (OW-P3) — page-session rollover bound and consecutive transient read failures.
    var pendingOldestStartMs: Long? = null,
    var pageRollovers: Int = 0,
    var readFailures: Int = 0,
    // Gutsy fork (GUT-217) — earliest start of an accepted record still open at the query's end.
    var pendingOpenStartMs: Long? = null
)

@Serializable
data class SyncState(
    val userKey: String,
    val fullExport: Boolean,
    val createdAt: Long,
    val connectionGeneration: String? = null,
    val queryFloorMs: Long? = null,
    val syncSessionId: String = java.util.UUID.randomUUID().toString(),
    var typeProgress: MutableMap<String, TypeSyncProgress> = mutableMapOf(),
    var totalSentCount: Int = 0,
    var completedTypes: MutableSet<String> = mutableSetOf(),
    var currentTypeIndex: Int = 0
) {
    val hasProgress: Boolean
        get() = totalSentCount > 0 || completedTypes.isNotEmpty()
}

/**
 * Manages health data synchronization.
 *
 * Works exclusively through the [HealthDataProvider] interface — all
 * provider-specific reading and unified-format conversion happens inside
 * the provider. The SyncManager just orchestrates timing, chunking,
 * auth retry, and payload delivery.
 */
class SyncManager(
    private val context: Context,
    private val secureStorage: SecureStorage,
    private val healthProvider: HealthDataProvider,
    private val dispatchers: DispatcherProvider,
    private val logger: (String) -> Unit,
    private val onAuthError: ((Int, String) -> Unit)? = null,
    // Gutsy fork — relayed to the host by the SDK singleton; null inside HealthSyncWorker.
    private val onAuthStateChanged: ((String) -> Unit)? = null,
    private val onAuthRecovered: ((String) -> Unit)? = null,
    // Gutsy fork (OW-P3) — injectable so the JVM tests drive the real run; production keeps the
    // shared client and schedules retries through WorkManager.
    private val httpClient: OkHttpClient = sharedHttpClient,
    retryScheduler: RetryScheduler? = null
) {
    companion object {
        val sharedHttpClient: OkHttpClient by lazy {
            OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(120, TimeUnit.SECONDS)
                .build()
        }
    }

    private val syncPrefs: SharedPreferences by lazy {
        context.getSharedPreferences(StorageKeys.SYNC_PREFS_NAME, Context.MODE_PRIVATE)
    }

    private val json = Json { ignoreUnknownKeys = true }

    private val dateFormatter: java.time.format.DateTimeFormatter =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(java.time.ZoneOffset.UTC)

    // Gutsy fork (OW-P3) — the persisted retry window, the attempt log and the one scheduled retry.
    // "Is a sync running" is process-wide now (SyncRunGate), not this instance's flag.
    internal var clock: () -> Long = System::currentTimeMillis
    internal var random: Random = Random.Default
    internal var nativePageBudget: Int = SyncDefaults.NATIVE_PAGE_BUDGET
    private val backoff: RetryBackoffStore by lazy { RetryBackoffStore(syncPrefs) { random } }
    private val attempts: SyncAttemptLog by lazy { SyncAttemptLog(syncPrefs) }
    private val scheduler: RetryScheduler by lazy { retryScheduler ?: WorkManagerRetryScheduler(context) }

    // Gutsy fork — refresh + recovery live here; the lock is process-wide (AuthRecovery.companion),
    // which a per-manager ReentrantLock never was: HealthSyncWorker builds its own SyncManager.
    private val authRecovery: AuthRecovery by lazy {
        AuthRecovery(secureStorage, httpClient, logger, onAuthStateChanged, onAuthRecovered, dispatchers.io)
    }

    private val stateMutex = Mutex()
    private var inMemoryState: SyncState? = null
    private var syncOwner: SecureStorage.ConnectionOwner? = null
    private fun requireSyncOwner(): SecureStorage.ConnectionOwner {
        val owner = syncOwner ?: error("Sync has no initiating owner")
        if (!secureStorage.isCurrent(owner)) throw SyncSupersededException()
        return owner
    }

    private var fullSyncStartTime: Long? = null
    private var currentLogsEndpoint: String? = null

    var syncIntervalMinutes: Long = SyncDefaults.SYNC_INTERVAL_MINUTES
        set(value) {
            field = maxOf(value, SyncDefaults.MIN_SYNC_INTERVAL_MINUTES)
        }

    // MARK: - Sync Start Timestamp

    /**
     * Computes the earliest epoch-ms timestamp to sync from, based on persisted `syncDaysBack`.
     * Returns the start of the day (midnight local time) that many days ago,
     * or `null` if full sync (no limit) is configured.
     */
    private fun syncStartTimestamp(): Long? {
        val daysBack = secureStorage.getSyncDaysBack()
        if (daysBack <= 0) return null
        val cal = java.util.Calendar.getInstance()
        cal.set(java.util.Calendar.HOUR_OF_DAY, 0)
        cal.set(java.util.Calendar.MINUTE, 0)
        cal.set(java.util.Calendar.SECOND, 0)
        cal.set(java.util.Calendar.MILLISECOND, 0)
        cal.add(java.util.Calendar.DAY_OF_YEAR, -daysBack)
        return cal.timeInMillis
    }

    // MARK: - User Key

    private fun userKey(): String {
        val userId = secureStorage.getUserId()
        return if (userId.isNullOrEmpty()) "user.none" else "user.$userId"
    }

    // MARK: - Auth

    private fun bearerValue(token: String): String =
        if (token.startsWith("Bearer ")) token else "Bearer $token"

    private fun applyAuth(requestBuilder: Request.Builder) {
        val accessToken = secureStorage.getAccessToken()
        val apiKey = secureStorage.getApiKey()
        if (accessToken != null) {
            requestBuilder.header("Authorization", bearerValue(accessToken))
        } else if (apiKey != null) {
            requestBuilder.header("X-Open-Wearables-API-Key", apiKey)
        }
    }

    private fun applyAuth(requestBuilder: Request.Builder, credential: String) {
        if (secureStorage.isApiKeyAuth) {
            requestBuilder.header("X-Open-Wearables-API-Key", credential)
        } else {
            requestBuilder.header("Authorization", bearerValue(credential))
        }
    }

    private fun emitAuthError(statusCode: Int) {
        val owner = syncOwner ?: return
        synchronized(SecureStorage.connectionLock) {
            if (!secureStorage.isCurrent(owner)) return
        logger("Auth error: HTTP $statusCode - token invalid")
        // Persist the verdict FIRST: the host may not be alive to receive the callback (this can run
        // inside HealthSyncWorker at 04:00), but it will read `getAuthState()` at its next launch.
        authRecovery.markReauthRequired()
        onAuthError?.invoke(statusCode, "Unauthorized - please re-authenticate")
        }
    }

    fun retryOutboxIfPossible() { /* reserved for future use */ }

    // MARK: - Background Sync

    suspend fun startBackgroundSync(host: String, customSyncUrl: String?): Boolean {
        schedulePeriodicSync(host, customSyncUrl)
        scheduleExpeditedSync(host, customSyncUrl)
        return true
    }

    private fun schedulePeriodicSync(host: String, customSyncUrl: String?) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val work = PeriodicWorkRequestBuilder<HealthSyncWorker>(
            syncIntervalMinutes, TimeUnit.MINUTES
        )
            .setConstraints(constraints)
            .setInputData(workDataOf(
                HealthSyncWorker.KEY_HOST to host,
                HealthSyncWorker.KEY_CUSTOM_SYNC_URL to customSyncUrl,
                HealthSyncWorker.KEY_TRIGGER to SyncTrigger.PERIODIC
            ))
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            SyncDefaults.WORK_NAME_PERIODIC,
            ExistingPeriodicWorkPolicy.UPDATE,
            work
        )
        logger("Scheduled periodic sync every $syncIntervalMinutes minute(s)")
    }

    fun scheduleExpeditedSync(host: String, customSyncUrl: String?) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val expeditedWork = OneTimeWorkRequestBuilder<HealthSyncWorker>()
            .setConstraints(constraints)
            .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
            .setInputData(workDataOf(
                HealthSyncWorker.KEY_HOST to host,
                HealthSyncWorker.KEY_CUSTOM_SYNC_URL to customSyncUrl,
                HealthSyncWorker.KEY_TRIGGER to SyncTrigger.EXPEDITED
            ))
            .build()

        WorkManager.getInstance(context).enqueueUniqueWork(
            SyncDefaults.WORK_NAME_EXPEDITED, ExistingWorkPolicy.REPLACE, expeditedWork
        )
        logger("Scheduled expedited sync")
    }

    fun stopBackgroundSync() {
        WorkManager.getInstance(context).cancelUniqueWork(SyncDefaults.WORK_NAME_PERIODIC)
        scheduler.cancel()
        logger("Cancelled periodic sync")
    }

    // MARK: - Sync Now

    /**
     * Gutsy fork (OW-P3, GUT-217 #6) — one run, in this order: owner fence, terminal auth states,
     * the persisted backoff window, the types readable NOW, the device lease, then read/upload rounds.
     * Runs are serialised process-wide ([SyncRunGate]). Nothing here throws for an expected failure
     * any more (an old `check()` could crash the SDK's foreground launch and told WorkManager
     * nothing); the [SyncOutcome] says what happens next, and a [SyncOutcome.RETRY] has already
     * scheduled its one retry.
     */
    suspend fun syncNow(host: String, customSyncUrl: String?, fullExport: Boolean, trigger: String = SyncTrigger.FOREGROUND): SyncOutcome {
        val owner = secureStorage.connectionOwner()
        return SyncRunGate.exclusive(owner, busy = {
            logger("Sync already in progress")
            SyncOutcome.BUSY
        }) {
            syncOwner = owner
            val attempt = SyncAttempt(java.util.UUID.randomUUID().toString(), userKey(), trigger, clock())
            attempts.save(attempt)
            var run = RunResult(SyncOutcome.RETRY, "unexpected")
            try {
                run = runOwned(host, customSyncUrl, fullExport, trigger, owner, attempt)
            } catch (e: SyncSupersededException) {
                logger("Sync superseded by a newer connection; progress retained")
                run = RunResult(SyncOutcome.SUPERSEDED, "superseded")
            } catch (e: CancellationException) {
                run = RunResult(SyncOutcome.CANCELLED, "cancelled")
                throw e
            } catch (e: Exception) {
                logger("Sync failed: ${e.javaClass.simpleName}: ${e.message}")
            } finally {
                settle(run, host, customSyncUrl, trigger, owner, attempt)
            }
            run.outcome
        }
    }

    private data class RunResult(val outcome: SyncOutcome, val code: String?, val httpStatus: Int? = null, val retryAfterSec: Long? = null)

    private fun terminal(code: String) = RunResult(SyncOutcome.TERMINAL, code)

    private suspend fun runOwned(
        host: String,
        customSyncUrl: String?,
        fullExport: Boolean,
        trigger: String,
        owner: SecureStorage.ConnectionOwner,
        attempt: SyncAttempt
    ): RunResult {
        requireSyncOwner()
        attempt.stage = SyncStage.AUTH
        val userId = owner.userId
        if (userId == null || !secureStorage.hasAuth) {
            logger("No credentials for sync")
            return terminal("no_credentials")
        }
        // Terminal auth states stop before any request: nothing native can repair them, and every
        // attempt used to spend a lease check (the shared 6/h renewal bucket) to learn it again.
        if (secureStorage.getAuthState() == AuthRecovery.STATE_REAUTH_REQUIRED) {
            logger("Sync stopped: re-authentication required")
            return terminal("reauth_required")
        }
        if (secureStorage.deviceLeaseRejected()) {
            logger("Sync stopped: device lease refused")
            return terminal("lease_rejected")
        }

        attempt.stage = SyncStage.GATE
        backoff.remainingMs(userKey(), clock(), SyncTrigger.isForeground(trigger))?.let { waitMs ->
            logger("Sync backing off for ${waitMs / 1000}s")
            attempt.nextAttemptAt = clock() + waitMs
            return RunResult(SyncOutcome.BACKED_OFF, "backed_off")
        }

        attempt.stage = SyncStage.TYPES
        val trackedTypes = healthProvider.getTrackedTypes().toList()
        attempt.trackedCount = trackedTypes.size
        if (trackedTypes.isEmpty()) {
            logger("No tracked types configured")
            return terminal("no_tracked_types")
        }
        // Local, before the lease check: a phone with nothing readable must not spend network.
        val readability = healthProvider.resolveReadableTypes(trackedTypes)
        requireSyncOwner()
        attempt.typeStatuses.putAll(readability.statuses)
        attempt.readableCount = readability.readable.size
        attempt.backgroundReadGranted = readability.backgroundReadGranted
        if (readability.transientFailure) return RunResult(SyncOutcome.RETRY, "permission_query_failed")
        val types = readability.readable
        if (types.isEmpty()) {
            val code = if (readability.providerUnavailable) "provider_unavailable" else "no_readable_types"
            logger("Sync stopped: $code (${trackedTypes.size} tracked)")
            return terminal(code)
        }
        if (types.size < trackedTypes.size) {
            val skipped = readability.statuses.entries.filter { it.value != TypeStatus.READABLE }
                .groupBy({ it.value }, { it.key }).entries.joinToString { "${it.key}=${it.value.size}" }
            logger("Reading ${types.size} of ${trackedTypes.size} tracked types ($skipped)")
        }

        attempt.stage = SyncStage.LEASE
        val lease = authRecovery.validateDeviceLeaseVerdict(owner)
        requireSyncOwner()
        when (lease.kind) {
            AuthRecovery.LeaseVerdict.Kind.ACTIVE -> Unit
            AuthRecovery.LeaseVerdict.Kind.RATE_LIMITED, AuthRecovery.LeaseVerdict.Kind.BACKED_OFF ->
                return RunResult(SyncOutcome.RETRY, "lease_rate_limited", retryAfterSec = lease.retryAfterSec)
            AuthRecovery.LeaseVerdict.Kind.TRANSIENT -> return RunResult(SyncOutcome.RETRY, "lease_unavailable")
            AuthRecovery.LeaseVerdict.Kind.REJECTED -> return terminal("lease_rejected")
            AuthRecovery.LeaseVerdict.Kind.INACTIVE -> return terminal("lease_inactive")
            AuthRecovery.LeaseVerdict.Kind.UNCONFIGURED -> return terminal("lease_unconfigured")
            AuthRecovery.LeaseVerdict.Kind.SUPERSEDED -> throw SyncSupersededException()
        }

        val endpoint = buildSyncEndpoint(host, customSyncUrl, userId)

        val existingState = stateMutex.withLock { loadSyncStateFromDisk() }
        val isResuming = existingState != null && existingState.hasProgress &&
            existingState.userKey == userKey() && existingState.connectionGeneration == owner.generation

        val floor = syncStartTimestamp()
        val floorLabel = if (floor != null) "since ${java.time.Instant.ofEpochMilli(floor)}" else "full history"

        val effectiveFullExport: Boolean
        if (isResuming) {
            effectiveFullExport = existingState!!.fullExport
            logger("Sync: resuming (${existingState.totalSentCount} sent, ${existingState.completedTypes.size}/${types.size} types done, $floorLabel)")
            stateMutex.withLock { inMemoryState = existingState }
        } else {
            effectiveFullExport = fullExport || !hasCompletedInitialSync()
            val mode = if (effectiveFullExport) "full export" else "incremental"
            logger("Sync: starting ($mode, ${types.size} types, ${healthProvider.providerName}, $floorLabel)")
            stateMutex.withLock {
                // The session's end comes from the clock that heals anchors (GUT-217).
                inMemoryState = SyncState(
                    userKey = userKey(), fullExport = effectiveFullExport,
                    createdAt = clock(), connectionGeneration = owner.generation, queryFloorMs = floor
                )
                persistStateToDisk()
            }
        }

        val syncStartTime = System.currentTimeMillis()
        val logsEndpoint = buildLogsEndpoint(host, userId)

        if (effectiveFullExport) {
            fullSyncStartTime = syncStartTime
            currentLogsEndpoint = logsEndpoint
            try {
                logger("Counting records for sync start log...")
                val stableFloor = inMemoryState?.queryFloorMs
                val typeCounts = countRecordsForTypes(types, stableFloor)
                logger("Sending sync start log to $logsEndpoint")
                sendSyncStartLog(logsEndpoint, types, typeCounts, stableFloor)
            } catch (e: SyncSupersededException) {
                throw e
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger("Sync start log failed: ${e.message}")
            }
        }

        val result = processTypesRoundRobin(types, effectiveFullExport, endpoint, attempt)

        if (effectiveFullExport && !result.completed) {
            val durationMs = (System.currentTimeMillis() - syncStartTime).toInt()
            for (typeResult in result.typeResults) {
                if (!typeResult.success && typeResult.recordCount > 0) {
                    sendTypeEndLog(logsEndpoint, typeResult.type, false, typeResult.recordCount, durationMs)
                }
            }
        }

        attempt.typeStatuses.putAll(result.statuses)
        val send = result.send
        return when {
            send != null -> uploadFailure(send)
            result.transientRead -> RunResult(SyncOutcome.RETRY, "read_transient")
            attempt.typeStatuses.values.any { SyncPolicy.isShortfall(it) } -> RunResult(SyncOutcome.PARTIAL, null)
            else -> RunResult(SyncOutcome.COMPLETED, null)
        }
    }

    /** A refused upload is retried only when the server or the network may answer differently later. */
    private fun uploadFailure(send: SendResult): RunResult {
        val kind = if (send.authTerminal) SyncPolicy.FailureKind.AUTH else SyncPolicy.classifyHttp(send.statusCode)
        return when (kind) {
            SyncPolicy.FailureKind.TRANSIENT -> RunResult(SyncOutcome.RETRY, when (send.statusCode) {
                null -> "upload_network"
                429 -> "upload_rate_limited"
                else -> "upload_transient"
            }, send.statusCode, send.retryAfterSec)
            SyncPolicy.FailureKind.AUTH -> RunResult(SyncOutcome.TERMINAL, "upload_auth", send.statusCode)
            SyncPolicy.FailureKind.REJECTED -> RunResult(SyncOutcome.TERMINAL, "upload_rejected", send.statusCode)
        }
    }

    /** Persist the retry window and schedule the one retry, or clear them; then close the attempt record. */
    private fun settle(
        run: RunResult,
        host: String,
        customSyncUrl: String?,
        trigger: String,
        owner: SecureStorage.ConnectionOwner,
        attempt: SyncAttempt
    ) {
        val now = clock()
        try {
            when (run.outcome) {
                SyncOutcome.RETRY -> if (secureStorage.isCurrent(owner)) {
                    val window = backoff.recordFailure(userKey(), run.retryAfterSec, now)
                    attempt.nextAttemptAt = window.nextAttemptAt
                    // The scheduled retry re-arms itself by appending, never by cancelling the running request.
                    scheduler.schedule(window.nextAttemptAt - now, host, customSyncUrl, replaceRunning = trigger != SyncTrigger.RETRY)
                    logger("Sync retry scheduled in ${(window.nextAttemptAt - now) / 1000}s (failure ${window.failures})")
                }
                SyncOutcome.COMPLETED, SyncOutcome.PARTIAL -> {
                    backoff.clear()
                    if (trigger != SyncTrigger.RETRY) scheduler.cancel()
                }
                // No retry for a state only the host or the member can change; the server's
                // Retry-After floor, if any, stays in force.
                SyncOutcome.TERMINAL -> if (trigger != SyncTrigger.RETRY) scheduler.cancel()
                else -> Unit
            }
        } catch (e: Exception) {
            logger("Retry scheduling unavailable: ${e.javaClass.simpleName}")
        }
        attempt.endedAt = now
        attempt.outcome = run.outcome.name.lowercase()
        attempt.errorCode = run.code
        attempt.httpStatus = run.httpStatus
        attempt.retryAfterSec = run.retryAfterSec
        if (run.outcome == SyncOutcome.COMPLETED || run.outcome == SyncOutcome.PARTIAL) attempt.stage = SyncStage.DONE
        try {
            attempts.save(attempt)
        } catch (e: Exception) {
            logger("Sync attempt record unavailable: ${e.javaClass.simpleName}")
        }
        fullSyncStartTime = null
        currentLogsEndpoint = null
        // Progress lives on disk between runs; a stale in-memory copy would misreport getSyncStatus().
        inMemoryState = null
    }

    // MARK: - Round-Robin Sync Orchestration (combined payloads)

    private data class TypeResult(val type: String, val success: Boolean, val recordCount: Int)
    private data class RoundRobinResult(
        val completed: Boolean,
        val totalRecords: Int,
        val typeResults: List<TypeResult>,
        /** The upload that stopped the run, when one did. */
        val send: SendResult? = null,
        /** A type failed transiently; the session is kept so it resumes. */
        val transientRead: Boolean = false,
        val statuses: Map<String, String> = emptyMap()
    )

    /** A type this run cannot read: the run continues without it and the type stays pending. */
    private class TypeSkip(val status: String) : Exception(status)

    private data class FetchResult(
        val type: String,
        val data: UnifiedHealthData = UnifiedHealthData(),
        val count: Int = 0,
        val nextCursor: Long? = null,
        val anchorTimestamp: Long? = null,
        val isDone: Boolean = false,
        val nativePage: Boolean = false,
        val nextPageToken: String? = null,
        val minStartMs: Long? = null,
        val openStartMs: Long? = null
    )

    private fun roundStatuses(types: List<String>, completed: Set<String>, skipped: Map<String, String>): Map<String, String> =
        types.associateWith { skipped[it] ?: if (it in completed) TypeStatus.COMPLETED else TypeStatus.PENDING }

    private suspend fun processTypesRoundRobin(
        types: List<String>,
        fullExport: Boolean,
        endpoint: String,
        attempt: SyncAttempt
    ): RoundRobinResult {
        val olderThanCursors = mutableMapOf<String, Long?>()
        val anchorCursors = mutableMapOf<String, Long?>()
        val completedTypes = mutableSetOf<String>()
        // Gutsy fork (OW-P3) — a type that cannot be read this run is skipped with its status; it is
        // never marked complete and never fails the round for the others.
        val skipped = linkedMapOf<String, String>()
        var transientRead = false

        stateMutex.withLock {
            val state = inMemoryState
            if (state != null) {
                completedTypes.addAll(state.completedTypes)
                for ((id, progress) in state.typeProgress) {
                    if (!progress.isComplete) {
                        progress.pendingOlderThan?.let { olderThanCursors[id] = it }
                        progress.pendingAnchorTimestamp?.let { anchorCursors[id] = it }
                    }
                }
            }
        }

        if (!fullExport) {
            val floor = syncStartTimestamp()
            for (type in types) {
                if (!completedTypes.contains(type) && !anchorCursors.containsKey(type)) {
                    val storedAnchor = anchorFor(type)
                    val anchor = when {
                        storedAnchor != null && floor != null -> maxOf(storedAnchor, floor)
                        storedAnchor != null -> storedAnchor
                        else -> floor
                    }
                    anchorCursors[type] = anchor
                }
            }
        }

        while (true) {
            val incompleteTypes = types.filter { !completedTypes.contains(it) && !skipped.containsKey(it) }
            if (incompleteTypes.isEmpty()) break

            val perTypeLimit = maxOf(1, SyncDefaults.CHUNK_SIZE / incompleteTypes.size)

            // Phase 1: Fetch one chunk from each type (no network yet)
            val roundResults = mutableListOf<FetchResult>()
            attempt.stage = SyncStage.READ

            for (type in incompleteTypes) {
                val result = try {
                    if (healthProvider.supportsNativePages) {
                        fetchNativePage(type, fullExport, perTypeLimit)
                    } else if (fullExport) {
                        fetchOneChunkNewestFirst(type, olderThanCursors[type], perTypeLimit)
                    } else {
                        fetchOneChunkIncremental(type, anchorCursors[type], perTypeLimit)
                    }
                } catch (e: SyncSupersededException) {
                    throw e
                } catch (e: CancellationException) {
                    throw e
                } catch (e: TypeSkip) {
                    logger("  $type: skipped this run (${e.status})")
                    skipped[type] = e.status
                    continue
                } catch (e: SecurityException) {
                    logger("  $type: read refused (permission revoked); skipped this run")
                    skipped[type] = TypeStatus.READ_DENIED
                    continue
                } catch (e: Exception) {
                    val status = stateMutex.withLock { noteReadFailure(type) }
                    logger("  $type: read failed (${e.javaClass.simpleName}); $status")
                    if (status == TypeStatus.READ_FAILED) transientRead = true
                    skipped[type] = status
                    continue
                }

                roundResults.add(result)

                if (result.isDone) {
                    completedTypes.add(type)
                } else {
                    if (fullExport) olderThanCursors[type] = result.nextCursor
                    else anchorCursors[type] = result.nextCursor
                }
            }

            // Phase 2: Merge all fetched data into one combined payload
            val mergedData = UnifiedHealthData(
                records = roundResults.flatMap { it.data.records },
                workouts = roundResults.flatMap { it.data.workouts },
                sleep = roundResults.flatMap { it.data.sleep }
            )

            if (!mergedData.isEmpty) {
                attempt.stage = SyncStage.UPLOAD
                val payload = buildPayload(mergedData)
                logPayloadSummary(mergedData)
                requireSyncOwner()
                val sendResult = sendPayload(endpoint, payload)
                requireSyncOwner()

                if (!sendResult.success) {
                    val reason = sendResult.statusCode?.let { "HTTP $it" } ?: "network error"
                    logger("Combined round failed ($reason)")
                    val (totalSent, typeResults) = roundTotals(types, persist = true)
                    val stateCompleted = types.filter { t -> typeResults.any { it.type == t && it.success } }.toSet()
                    return RoundRobinResult(false, totalSent, typeResults, send = sendResult,
                        statuses = roundStatuses(types, stateCompleted, skipped))
                }

                attempt.rounds += 1
                attempt.sentItems += mergedData.totalCount
                logger("Round sent: ${mergedData.totalCount} items (${sendResult.payloadSizeKb} KB) -> ${sendResult.statusCode}")
            }

            // Phase 3: Update progress for all types in this round
            attempt.stage = SyncStage.COMMIT
            val newlyCompletedTypes = mutableListOf<Pair<String, Int>>()
            stateMutex.withLock {
                synchronized(SecureStorage.connectionLock) {
                requireSyncOwner()
                for (result in roundResults) {
                    updateInMemoryProgress(result.type, result.count, isComplete = result.isDone, anchorTimestamp = result.anchorTimestamp,
                        openStartMs = result.openStartMs)
                    inMemoryState?.typeProgress?.get(result.type)?.readFailures = 0
                    if (result.nativePage) {
                        inMemoryState?.typeProgress?.get(result.type)?.let { progress ->
                            result.nextPageToken?.let { progress.nativeTokenHashes.add(nativeTokenHash(it)) }
                            progress.nativePageToken = result.nextPageToken
                            progress.nativePageCount += 1
                            progress.pendingOldestStartMs = listOfNotNull(progress.pendingOldestStartMs, result.minStartMs).minOrNull()
                        }
                    }
                    if (fullExport && !result.isDone) {
                        inMemoryState?.typeProgress?.get(result.type)?.pendingOlderThan = result.nextCursor
                    }
                    if (result.isDone) {
                        newlyCompletedTypes.add(result.type to (inMemoryState?.typeProgress?.get(result.type)?.sentCount ?: 0))
                    }
                }
                persistStateToDisk()
                }
            }

            val startTime = fullSyncStartTime
            val logEndpoint = currentLogsEndpoint
            if (fullExport && startTime != null && logEndpoint != null && newlyCompletedTypes.isNotEmpty()) {
                for ((type, count) in newlyCompletedTypes) {
                    if (count > 0) {
                        val durationMs = (System.currentTimeMillis() - startTime).toInt()
                        logger("Sending sync end log: ${payloadTypeName(type)} ($count records, ${durationMs}ms)")
                        try {
                            sendTypeEndLog(logEndpoint, type, true, count, durationMs)
                        } catch (e: Exception) {
                            logger("Type end log failed for $type: ${e.message}")
                        }
                    }
                }
            }
        }

        if (transientRead) {
            // Keep the session: the failed types resume their page tokens on the scheduled retry.
            val (totalSent, typeResults) = roundTotals(types, persist = true)
            return RoundRobinResult(false, totalSent, typeResults, transientRead = true,
                statuses = roundStatuses(types, completedTypes, skipped))
        }

        // Types skipped for permission or budget reasons stay pending (no anchor, no completion);
        // once readable, an incremental run reads them from the floor.
        val (totalSent, typeResults) = stateMutex.withLock {
            val state = inMemoryState
            val sent = state?.totalSentCount ?: 0
            val results = types.map { type ->
                TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
            }
            synchronized(SecureStorage.connectionLock) {
                requireSyncOwner()
                if (state?.fullExport == true) markFullExportDone()
                if (state != null) logger("Native traversal accepted ($sent items, ${state.completedTypes.size} types" +
                    (if (skipped.isNotEmpty()) ", ${skipped.size} skipped" else "") + "); server commit unverified")
                clearSyncSessionInternal()
            }
            Pair(sent, results)
        }
        return RoundRobinResult(true, totalSent, typeResults, statuses = roundStatuses(types, completedTypes, skipped))
    }

    private suspend fun roundTotals(types: List<String>, persist: Boolean): Pair<Int, List<TypeResult>> = stateMutex.withLock {
        if (persist) persistStateToDisk()
        val state = inMemoryState
        val results = types.map { type ->
            TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
        }
        Pair(state?.totalSentCount ?: 0, results)
    }

    /** Consecutive transient read failures; past the limit the type is parked so the session can finish. */
    private fun noteReadFailure(type: String): String {
        val progress = inMemoryState?.typeProgress?.getOrPut(type) { TypeSyncProgress(type) } ?: return TypeStatus.READ_FAILED
        progress.readFailures += 1
        return if (progress.readFailures >= SyncDefaults.TYPE_READ_FAILURE_LIMIT) TypeStatus.READ_FAILED_REPEATEDLY
        else TypeStatus.READ_FAILED
    }

    // MARK: - Fetch-Only Chunk Processors (no network)

    /** Stable native query + opaque token. Only accepted rounds persist the next token/watermark. */
    private fun nativeTokenHash(value: String): String = java.security.MessageDigest.getInstance("SHA-256")
        .digest(value.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }

    private suspend fun fetchNativePage(type: String, fullExport: Boolean, limit: Int): FetchResult {
        val (query, token) = stateMutex.withLock {
            val state = inMemoryState ?: error("Native traversal has no owner state")
            val progress = state.typeProgress.getOrPut(type) { TypeSyncProgress(type) }
            if (progress.queryStartMs == null) {
                val floor = state.queryFloorMs ?: 0L
                // Gutsy fork (GUT-217): a window never starts past the session's end.
                val start = if (fullExport) floor else maxOf(anchorFor(type) ?: floor, floor)
                progress.queryStartMs = minOf(start, state.createdAt)
                progress.queryEndMs = state.createdAt
                progress.nativePageSize = minOf(limit, 200)
                // Persist the query before a first page: resume must not move its time bounds.
                persistStateToDisk()
            }
            // Gutsy fork (OW-P3): an exhausted page session rolls over instead of refusing every resume.
            if (progress.nativePageCount >= nativePageBudget) rollOverPageSession(type, progress, fullExport)
            Pair(ProviderPageQuery(progress.queryStartMs!!, progress.queryEndMs!!,
                progress.nativePageSize!!, !fullExport), progress.nativePageToken)
        }
        // Gutsy fork (GUT-217): Health Connect refuses a window whose start is not before its end
        // (TimeRangeFilter.between). Nothing between the anchor and the session's end is complete.
        if (query.startMs >= query.endMs) {
            logger("  $type: no new data")
            return FetchResult(type = type, isDone = true)
        }
        requireSyncOwner()
        val result = healthProvider.readPage(type, query, token)
        requireSyncOwner()
        // An id with no native reader is a skipped type; it used to fail the round before any upload.
        if (!result.nativePage || result.nativeRecordCount == null) throw TypeSkip(TypeStatus.UNSUPPORTED)
        if (result.data.totalCount > 50000) throw TypeSkip(TypeStatus.SAMPLE_BUDGET_EXCEEDED)
        val seen = inMemoryState?.typeProgress?.get(type)?.nativeTokenHashes ?: emptySet()
        if (result.nextPageToken != null && (result.nextPageToken == token || nativeTokenHash(result.nextPageToken) in seen)) {
            // A cycling token restarts this type's page session from its committed point next time.
            stateMutex.withLock {
                inMemoryState?.typeProgress?.get(type)?.let { resetPageSession(it) }
                persistStateToDisk()
            }
            error("Native token cycle")
        }
        val previousMax = inMemoryState?.typeProgress?.get(type)?.pendingAnchorTimestamp
        val watermark = listOfNotNull(previousMax, result.maxTimestamp).maxOrNull()
        return FetchResult(type, result.data, result.nativeRecordCount,
            anchorTimestamp = watermark, isDone = result.nextPageToken == null,
            nativePage = true, nextPageToken = result.nextPageToken, minStartMs = result.minStartTimestamp,
            openStartMs = result.openStartTimestamp)
    }

    /**
     * A page session at its budget is committed and restarted, never abandoned: incremental
     * (ascending) work commits its accepted watermark as the type anchor — the point a finished
     * session would commit — and continues from there; historical (descending) work narrows its
     * upper bound to the oldest accepted start. A session that cannot move parks the type.
     */
    private fun rollOverPageSession(type: String, progress: TypeSyncProgress, fullExport: Boolean) {
        val anchor = SyncPolicy.committableAnchor(progress.pendingAnchorTimestamp, progress.queryEndMs!!, progress.pendingOpenStartMs)
        val bounds = SyncPolicy.rolloverBounds(progress.queryStartMs!!, progress.queryEndMs!!, !fullExport,
            anchor, progress.pendingOldestStartMs) ?: throw TypeSkip(TypeStatus.PAGE_BUDGET_STALLED)
        if (!fullExport) anchor?.let { saveAnchor(type, it) }
        progress.queryStartMs = bounds.first
        progress.queryEndMs = bounds.second
        progress.nativePageToken = null
        progress.nativeTokenHashes.clear()
        progress.nativePageCount = 0
        progress.pageRollovers += 1
        persistStateToDisk()
        logger("  $type: page budget reached; session rolled over (#${progress.pageRollovers})")
    }

    private fun resetPageSession(progress: TypeSyncProgress) {
        progress.nativePageToken = null
        progress.nativeTokenHashes.clear()
        progress.nativePageCount = 0
        progress.queryStartMs = null
        progress.queryEndMs = null
        progress.nativePageSize = null
        progress.pendingOldestStartMs = null
    }

    private suspend fun fetchOneChunkNewestFirst(
        type: String,
        olderThan: Long?,
        limit: Int
    ): FetchResult {
        val floor = syncStartTimestamp()
        val floorIso = floor?.let { UnifiedTimestamp.fromEpochMs(it) }

        logger("  $type: querying (newest first, limit=$limit${olderThan?.let { ", olderThan=${java.time.Instant.ofEpochMilli(it)}" } ?: ""})...")

        val result = healthProvider.readDataDescending(type, olderThan, limit)

        if (result.data.isEmpty) {
            logger("  $type: all data sent (newest first)")
            return FetchResult(type = type, isDone = true)
        }

        val reachedFloor = floor != null && result.minTimestamp != null && result.minTimestamp <= floor
        val isLastChunk = result.data.totalCount < limit || reachedFloor

        val data = if (reachedFloor && floorIso != null) result.data.filterSince(floorIso) else result.data

        if (data.isEmpty) {
            logger("  $type: all data within range sent")
            return FetchResult(type = type, isDone = true)
        }

        val anchorTs = if (olderThan == null) result.maxTimestamp else null
        val nextOlderThan = if (isLastChunk) null else result.minTimestamp

        logger("  $type: ${data.totalCount} samples (newest first)")

        return FetchResult(
            type = type, data = data, count = data.totalCount,
            nextCursor = nextOlderThan, anchorTimestamp = anchorTs, isDone = isLastChunk
        )
    }

    private suspend fun fetchOneChunkIncremental(
        type: String,
        anchor: Long?,
        limit: Int
    ): FetchResult {
        // Gutsy fork (GUT-217): a cursor at or past the session's end (a still-open record's end)
        // has nothing to read, and Samsung Health's since-to-now window would be inverted.
        val end = inMemoryState?.createdAt ?: error("Incremental read has no owned session")
        if (anchor != null && anchor >= end) {
            logger("  $type: no new data")
            return FetchResult(type = type, isDone = true)
        }
        logger("  $type: querying (limit=$limit)...")

        val result = healthProvider.readData(type, anchor, limit)

        if (result.data.isEmpty) {
            logger("  $type: no new data")
            return FetchResult(type = type, isDone = true)
        }

        val count = result.data.totalCount
        val isLastChunk = count < limit

        logger("  $type: $count samples")

        return FetchResult(
            type = type, data = result.data, count = count,
            nextCursor = result.maxTimestamp, anchorTimestamp = result.maxTimestamp,
            isDone = isLastChunk
        )
    }

    private fun updateInMemoryProgress(typeIdentifier: String, sentInChunk: Int, isComplete: Boolean, anchorTimestamp: Long?,
                                       openStartMs: Long? = null) {
        val state = inMemoryState ?: return
        val progress = state.typeProgress.getOrPut(typeIdentifier) { TypeSyncProgress(typeIdentifier) }
        progress.sentCount += sentInChunk
        progress.isComplete = isComplete
        if (anchorTimestamp != null) progress.pendingAnchorTimestamp = anchorTimestamp
        progress.pendingOpenStartMs = listOfNotNull(progress.pendingOpenStartMs, openStartMs).minOrNull()
        state.totalSentCount += sentInChunk
        if (isComplete) {
            state.completedTypes.add(typeIdentifier)
            // Gutsy fork (GUT-217): never past the session's end nor a still-open record's start.
            SyncPolicy.committableAnchor(progress.pendingAnchorTimestamp, progress.queryEndMs ?: state.createdAt,
                progress.pendingOpenStartMs)?.let { saveAnchor(typeIdentifier, it) }
        }
    }

    // MARK: - Payload (unified)

    private fun buildPayload(data: UnifiedHealthData): Map<String, Any> = mutableMapOf<String, Any>(
        "provider" to healthProvider.providerId,
        "sdkVersion" to SyncDefaults.SDK_VERSION,
        "syncTimestamp" to UnifiedTimestamp.fromEpochMs(System.currentTimeMillis()),
        "syncType" to if (inMemoryState?.fullExport == true) "historical" else "live",
        "data" to data.toDataMap()
    ).apply {
        inMemoryState?.takeIf { it.fullExport }?.let { put("syncSessionId", it.syncSessionId) }
    }

    // MARK: - Payload Summary Logging

    private fun logPayloadSummary(data: UnifiedHealthData) {
        val typeCounts = mutableMapOf<String, Int>()

        for (r in data.records) {
            typeCounts[r.type] = (typeCounts[r.type] ?: 0) + 1
        }
        if (data.sleep.isNotEmpty()) {
            typeCounts["sleep"] = data.sleep.size
        }
        if (data.workouts.isNotEmpty()) {
            typeCounts["workouts"] = data.workouts.size
        }

        val totalCount = typeCounts.values.sum()
        val breakdown = typeCounts.entries
            .sortedByDescending { it.value }
            .joinToString(", ") { "${it.key}: ${it.value}" }

        logger("Sending $totalCount items ($breakdown)")
    }

    // MARK: - Token Refresh

    private enum class TokenRefreshResult {
        SUCCESS, AUTH_FAILURE, NETWORK_ERROR
    }

    /**
     * Gutsy fork: delegated to [AuthRecovery] — process-wide single-flight, refresh-first verified
     * persistence, the double-check on rejection, and native renewal. Same result contract as before.
     */
    private suspend fun attemptTokenRefresh(): TokenRefreshResult =
        when (authRecovery.refreshTokens()) {
            AuthRecovery.Result.SUCCESS -> TokenRefreshResult.SUCCESS
            AuthRecovery.Result.AUTH_FAILURE -> TokenRefreshResult.AUTH_FAILURE
            AuthRecovery.Result.NETWORK_ERROR -> TokenRefreshResult.NETWORK_ERROR
        }

    // MARK: - Send with Auth Retry

    private data class SendResult(
        val success: Boolean,
        val statusCode: Int?,
        val payloadSizeKb: Int,
        // Gutsy fork (OW-P3) — what the run needs to choose between retry and stop.
        val retryAfterSec: Long? = null,
        val authTerminal: Boolean = false
    )

    private suspend fun sendPayload(endpoint: String, payload: Map<String, Any>): SendResult {
        val body = streamingJsonBody(payload)
        return sendWithBody(endpoint, body)
    }

    /**
     * Creates an OkHttp RequestBody with the JSON payload fully buffered so a
     * concrete Content-Length is sent. Chunked transfer encoding (the previous
     * streaming approach) is rejected by the OpenLiteSpeed proxy in front of
     * the OW backend; the buffer also lets OkHttp transparently retry the body.
     * JsonWriter is still used for serialization to avoid an intermediate
     * JsonElement tree.
     */
    private fun streamingJsonBody(payload: Map<String, Any>): RequestBody {
        val baos = java.io.ByteArrayOutputStream()
        val writer = android.util.JsonWriter(
            java.io.OutputStreamWriter(baos, Charsets.UTF_8)
        )
        writeValue(writer, payload)
        writer.flush()
        writer.close()
        
        val bytes = baos.toByteArray()
        
        return object : okhttp3.RequestBody() {
            override fun contentType() = "application/json".toMediaType()
            override fun contentLength(): Long = bytes.size.toLong()
            override fun writeTo(sink: okio.BufferedSink) {
                sink.write(bytes)
            }
        }
    }

    private fun writeValue(writer: android.util.JsonWriter, value: Any?) {
        when (value) {
            null -> writer.nullValue()
            is Boolean -> writer.value(value)
            is Int -> writer.value(value.toLong())
            is Long -> writer.value(value)
            is Float -> writer.value(value.toDouble())
            is Double -> writer.value(value)
            is Number -> writer.value(value.toDouble())
            is String -> writer.value(value)
            is Map<*, *> -> {
                writer.beginObject()
                for ((k, v) in value) {
                    writer.name(k.toString())
                    writeValue(writer, v)
                }
                writer.endObject()
            }
            is List<*> -> {
                writer.beginArray()
                for (item in value) {
                    writeValue(writer, item)
                }
                writer.endArray()
            }
            else -> writer.value(value.toString())
        }
    }

    private suspend fun sendWithBody(endpoint: String, body: okhttp3.RequestBody): SendResult =
        withContext(dispatchers.io) {
            try {
                val owner = requireSyncOwner()
                val requestBuilder = Request.Builder()
                    .url(endpoint)
                    .post(body)
                    .header("Content-Type", "application/json")
                secureStorage.withConnection(owner) { applyAuth(requestBuilder) }
                    ?: error("Upload connection superseded")
                requireSyncOwner()
                val response = httpClient.newCall(requestBuilder.build()).execute()
                val sizeKb = (response.header("Content-Length")?.toLongOrNull() ?: 0L) / 1024
                val code = response.code
                response.body?.close()

                requireSyncOwner()
                if (response.isSuccessful) return@withContext SendResult(true, code, sizeKb.toInt())
                if (code == 401) {
                    logger("Got 401, refreshing token...")
                    return@withContext handle401(endpoint, body, sizeKb.toInt())
                }

                SendResult(false, code, sizeKb.toInt(), retryAfterOf(response, code))
            } catch (e: SyncSupersededException) {
                throw e
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger("Upload error: ${e.javaClass.simpleName}: ${e.message}")
                SendResult(false, null, 0)
            }
        }

    private fun retryAfterOf(response: okhttp3.Response, code: Int): Long? =
        if (code == 429 || code == 503) SyncPolicy.parseRetryAfterSeconds(response.header("Retry-After"), clock()) else null

    /** Gutsy fork (OW-P3): tells a refused credential (stop) from a refresh that could not reach the server (retry). */
    private suspend fun handle401(endpoint: String, body: okhttp3.RequestBody, sizeKb: Int): SendResult {
        requireSyncOwner()
        if (secureStorage.isApiKeyAuth) {
            emitAuthError(401)
            return SendResult(false, 401, sizeKb, authTerminal = true)
        }

        when (attemptTokenRefresh()) {
            TokenRefreshResult.SUCCESS -> {
                requireSyncOwner()
                val newCredential = secureStorage.authCredential ?: return SendResult(false, 401, sizeKb)
                logger("Token refreshed, retrying...")
                return try {
                    val retryBuilder = Request.Builder()
                        .url(endpoint)
                        .post(body)
                        .header("Content-Type", "application/json")
                    applyAuth(retryBuilder, newCredential)

                    requireSyncOwner()
                    val retryResponse = httpClient.newCall(retryBuilder.build()).execute()
                    val retryCode = retryResponse.code
                    retryResponse.body?.close()
                    requireSyncOwner()
                    if (retryResponse.isSuccessful) {
                        logger("Retry: HTTP $retryCode")
                        SendResult(true, retryCode, sizeKb)
                    } else {
                        logger("Retry failed: HTTP $retryCode")
                        val refused = retryCode in 401..403
                        if (refused) emitAuthError(401)
                        SendResult(false, retryCode, sizeKb, retryAfterOf(retryResponse, retryCode), authTerminal = refused)
                    }
                } catch (e: SyncSupersededException) {
                    throw e
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger("Retry failed: ${e.message}")
                    SendResult(false, null, sizeKb)
                }
            }
            TokenRefreshResult.AUTH_FAILURE -> {
                requireSyncOwner()
                emitAuthError(401)
                return SendResult(false, 401, sizeKb, authTerminal = true)
            }
            TokenRefreshResult.NETWORK_ERROR -> {
                logger("Token refresh failed (network) - will retry later")
                return SendResult(false, null, sizeKb)
            }
        }
    }

    // MARK: - Sync Logging

    private fun payloadTypeName(trackedTypeId: String): String = when (trackedTypeId) {
        "steps" -> "STEP_COUNT"
        "heartRate" -> "HEART_RATE"
        "restingHeartRate" -> "RESTING_HEART_RATE"
        "heartRateVariabilitySDNN" -> "HEART_RATE_VARIABILITY"
        "oxygenSaturation" -> "OXYGEN_SATURATION"
        "bloodPressure", "bloodPressureSystolic" -> "BLOOD_PRESSURE_SYSTOLIC"
        "bloodPressureDiastolic" -> "BLOOD_PRESSURE_DIASTOLIC"
        "bloodGlucose" -> "BLOOD_GLUCOSE"
        "activeEnergy" -> "ACTIVE_CALORIES_BURNED"
        "basalEnergy" -> "BASAL_METABOLIC_RATE"
        "bodyTemperature" -> "BODY_TEMPERATURE"
        "bodyMass" -> "WEIGHT"
        "height" -> "HEIGHT"
        "bodyFatPercentage" -> "BODY_FAT"
        "leanBodyMass" -> "LEAN_BODY_MASS"
        "flightsClimbed" -> "FLOORS_CLIMBED"
        "distanceWalkingRunning", "distanceCycling" -> "DISTANCE"
        "water", "dietaryWater" -> "HYDRATION"
        "vo2Max" -> "VO2_MAX"
        "respiratoryRate" -> "RESPIRATORY_RATE"
        "workout" -> "WORKOUT"
        "sleep" -> "SLEEP"
        else -> trackedTypeId.uppercase()
    }

    private fun buildLogsEndpoint(host: String, userId: String): String {
        val h = if (host.endsWith("/")) host.dropLast(1) else host
        return "$h/api/v1/sdk/users/$userId/logs"
    }

    private fun collectDeviceState(): Map<String, Any> {
        val batteryManager = context.getSystemService(Context.BATTERY_SERVICE) as? android.os.BatteryManager
        val powerManager = context.getSystemService(Context.POWER_SERVICE) as? android.os.PowerManager
        val activityManager = context.getSystemService(Context.ACTIVITY_SERVICE) as? android.app.ActivityManager

        val batteryLevel = batteryManager
            ?.getIntProperty(android.os.BatteryManager.BATTERY_PROPERTY_CAPACITY)
            ?.let { it / 100f } ?: -1f

        val batteryIntent = try {
            context.registerReceiver(null, android.content.IntentFilter(android.content.Intent.ACTION_BATTERY_CHANGED))
        } catch (_: Exception) { null }
        val status = batteryIntent?.getIntExtra(android.os.BatteryManager.EXTRA_STATUS, -1) ?: -1
        val batteryState = when (status) {
            android.os.BatteryManager.BATTERY_STATUS_CHARGING -> "CHARGING"
            android.os.BatteryManager.BATTERY_STATUS_FULL -> "FULL"
            android.os.BatteryManager.BATTERY_STATUS_DISCHARGING -> "DISCHARGING"
            android.os.BatteryManager.BATTERY_STATUS_NOT_CHARGING -> "NOT_CHARGING"
            else -> "UNKNOWN"
        }

        val isLowPower = powerManager?.isPowerSaveMode ?: false

        val thermalState = if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.Q) {
            when (powerManager?.currentThermalStatus) {
                android.os.PowerManager.THERMAL_STATUS_NONE -> "NONE"
                android.os.PowerManager.THERMAL_STATUS_LIGHT -> "LIGHT"
                android.os.PowerManager.THERMAL_STATUS_MODERATE -> "MODERATE"
                android.os.PowerManager.THERMAL_STATUS_SEVERE -> "SEVERE"
                android.os.PowerManager.THERMAL_STATUS_CRITICAL -> "CRITICAL"
                android.os.PowerManager.THERMAL_STATUS_EMERGENCY -> "EMERGENCY"
                android.os.PowerManager.THERMAL_STATUS_SHUTDOWN -> "SHUTDOWN"
                else -> "UNKNOWN"
            }
        } else "UNSUPPORTED"

        val memInfo = android.app.ActivityManager.MemoryInfo()
        activityManager?.getMemoryInfo(memInfo)

        return mapOf(
            "eventType" to "device_state",
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "batteryLevel" to batteryLevel,
            "batteryState" to batteryState,
            "isLowPowerMode" to isLowPower,
            "thermalState" to thermalState,
            "taskType" to "background",
            "availableRamBytes" to memInfo.availMem,
            "totalRamBytes" to memInfo.totalMem
        )
    }

    private suspend fun countRecordsForTypes(types: List<String>, sinceTimestamp: Long?): Map<String, Int> {
        val counts = mutableMapOf<String, Int>()
        val end = inMemoryState?.createdAt ?: error("Count has no owned query")
        for (type in types) {
            // Gutsy fork (GUT-217): an empty window counts nothing; Health Connect refuses to read it.
            if ((sinceTimestamp ?: 0L) >= end) { counts[type] = 0; continue }
            try {
                var count = 0
                var token: String? = null
                val seen = mutableSetOf<String>()
                var pages = 0
                do {
                    requireSyncOwner()
                    check(++pages <= nativePageBudget) { "Count traversal budget exceeded" }
                    val result = healthProvider.readPage(type, ProviderPageQuery(sinceTimestamp ?: 0L, end, 200, true), token)
                    requireSyncOwner()
                    check(result.nativePage && result.nativeRecordCount != null) { "Native count unavailable" }
                    count += result.nativeRecordCount
                    token = result.nextPageToken
                    check(token == null || seen.add(token)) { "Count traversal token cycle" }
                } while (token != null)
                counts[type] = count
            } catch (e: SyncSupersededException) {
                throw e
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Gutsy fork (OW-P3): one uncountable type no longer suppresses the whole start log.
                logger("  $type: count unavailable (${e.message})")
            }
        }
        return counts
    }

    private suspend fun sendSyncStartLog(logsEndpoint: String, types: List<String>, typeCounts: Map<String, Int>, startTimestamp: Long?) {
        val dataTypeCounts = types.mapNotNull { type -> typeCounts[type]?.let { mapOf("type" to payloadTypeName(type), "count" to it) } }

        val timeRange = mutableMapOf<String, String>(
            "endDate" to dateFormatter.format(java.time.Instant.ofEpochMilli(inMemoryState?.createdAt ?: error("Historical query unavailable")))
        )
        startTimestamp?.let {
            timeRange["startDate"] = dateFormatter.format(java.time.Instant.ofEpochMilli(it))
        }

        val startEvent: Map<String, Any> = mapOf(
            "eventType" to "historical_data_sync_start",
            "syncSessionId" to (inMemoryState?.syncSessionId ?: error("Historical session missing")),
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "dataTypeCounts" to dataTypeCounts,
            "timeRange" to timeRange
        )

        val body: Map<String, Any> = mapOf(
            "sdkVersion" to SyncDefaults.SDK_VERSION,
            "provider" to healthProvider.providerId,
            "syncType" to "historical",
            "syncSessionId" to (inMemoryState?.syncSessionId ?: error("Historical session missing")),
            "events" to listOf(startEvent, collectDeviceState())
        )

        sendSyncLog(logsEndpoint, body)
    }

    private suspend fun sendTypeEndLog(logsEndpoint: String, type: String, success: Boolean, recordCount: Int, durationMs: Int) {
        val endEvent: Map<String, Any> = mapOf(
            "eventType" to "historical_data_type_sync_end",
            "syncSessionId" to (inMemoryState?.syncSessionId ?: error("Historical session missing")),
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "dataType" to payloadTypeName(type),
            "success" to success,
            "recordCount" to recordCount,
            "durationMs" to durationMs
        )

        val body: Map<String, Any> = mapOf(
            "sdkVersion" to SyncDefaults.SDK_VERSION,
            "provider" to healthProvider.providerId,
            "syncType" to "historical",
            "syncSessionId" to (inMemoryState?.syncSessionId ?: error("Historical session missing")),
            "events" to listOf(endEvent, collectDeviceState())
        )

        sendSyncLog(logsEndpoint, body)
    }

    private suspend fun sendSyncLog(endpoint: String, body: Map<String, Any>) = withContext(dispatchers.io) {
        try {
            val requestBody = streamingJsonBody(body)
            val requestBuilder = Request.Builder()
                .url(endpoint)
                .post(requestBody)
                .header("Content-Type", "application/json")
            val owner = requireSyncOwner()
            secureStorage.withConnection(owner) { applyAuth(requestBuilder) } ?: error("Log connection superseded")
            requireSyncOwner()
            val response = httpClient.newCall(requestBuilder.build()).execute()
            response.body?.close()
            logger("Sync log: HTTP ${response.code}")
        } catch (e: Exception) {
            logger("Sync log error: ${e.message}")
        }
    }

    // MARK: - Sync Endpoint

    private fun buildSyncEndpoint(host: String, customSyncUrl: String?, userId: String): String {
        if (customSyncUrl != null) {
            if (customSyncUrl.contains("{user_id}") || customSyncUrl.contains("{userId}")) {
                return customSyncUrl
                    .replace("{userId}", userId)
                    .replace("{user_id}", userId)
            }
            val normalizedBase = customSyncUrl.trimEnd('/')
            return "$normalizedBase/sdk/users/$userId/sync"
        }
        val h = if (host.endsWith("/")) host.dropLast(1) else host
        return "$h/api/v1/sdk/users/$userId/sync"
    }

    // MARK: - Anchors

    private fun loadAnchors(): Map<String, Long> {
        val jsonStr = syncPrefs.getString(StorageKeys.KEY_ANCHORS, null) ?: return emptyMap()
        return try {
            val map = json.decodeFromString<Map<String, Double>>(jsonStr)
            map.mapValues { it.value.toLong() }
        } catch (_: Exception) { emptyMap() }
    }

    /** Gutsy fork (GUT-217): the type's stored anchor, healed when it lies in the future ([SyncPolicy.healedAnchor]). */
    private fun anchorFor(type: String): Long? {
        val stored = loadAnchors()[type] ?: return null
        val healed = SyncPolicy.healedAnchor(stored, clock(), java.time.ZoneId.systemDefault())
        if (healed != stored) {
            logger("  $type: anchor ${java.time.Instant.ofEpochMilli(stored)} is in the future; reading from ${java.time.Instant.ofEpochMilli(healed)}")
        }
        return healed
    }

    private fun saveAnchor(type: String, timestamp: Long) = synchronized(SecureStorage.connectionLock) {
        requireSyncOwner()
        val current = loadAnchors().toMutableMap()
        current[type] = timestamp
        syncPrefs.edit().putString(
            StorageKeys.KEY_ANCHORS,
            json.encodeToString(current.mapValues { it.value.toDouble() })
        ).apply()
    }

    fun resetAnchors() {
        syncPrefs.edit()
            .remove(StorageKeys.KEY_ANCHORS)
            .putBoolean(fullDoneKey(), false)
            .apply()
        clearSyncSession()
        logger("Anchors reset - will perform full sync on next sync")
    }

    private fun fullDoneKey(): String = "fullDone.${userKey()}"
    private fun hasCompletedInitialSync(): Boolean = syncPrefs.getBoolean(fullDoneKey(), false)
    private fun markFullExportDone() { syncPrefs.edit().putBoolean(fullDoneKey(), true).apply() }

    // MARK: - Sync State (Mutex-protected disk I/O)

    private fun syncStateDir(): File = File(context.filesDir, StorageKeys.SYNC_STATE_DIR).also { if (!it.exists()) it.mkdirs() }
    private fun syncStateFile(): File = File(syncStateDir(), StorageKeys.SYNC_STATE_FILE)

    private fun persistStateToDisk() = synchronized(SecureStorage.connectionLock) {
        requireSyncOwner()
        val state = inMemoryState ?: return@synchronized
        try {
            val jsonStr = json.encodeToString(state)
            if (jsonStr.isNotBlank() && jsonStr.startsWith("{")) {
                val file = syncStateFile()
                val tempFile = File(file.parent, "${file.name}.tmp")
                tempFile.writeText(jsonStr)
                // Gutsy fork (OW-P3): replace atomically; File.renameTo returns false and keeps the
                // stale state wherever the filesystem refuses to overwrite an existing target.
                java.nio.file.Files.move(tempFile.toPath(), file.toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
            }
        } catch (e: Exception) {
            logger("Failed to save sync state: ${e.message}")
        }
    }

    private fun loadSyncStateFromDisk(): SyncState? {
        return try {
            val file = syncStateFile()
            if (!file.exists()) return null
            val jsonStr = file.readText()
            if (jsonStr.isBlank()) { file.delete(); return null }
            val state = json.decodeFromString<SyncState>(jsonStr)
            if (state.userKey != userKey()) { clearSyncSessionInternal(); return null }
            state
        } catch (e: Exception) {
            logger("Corrupted sync state, clearing: ${e.message}")
            try { syncStateFile().delete() } catch (_: Exception) {}
            null
        }
    }

    private fun clearSyncSessionInternal() {
        inMemoryState = null
        try { syncStateFile().delete() } catch (_: Exception) {}
    }

    fun getSyncStatus(): Map<String, Any?> {
        val state = inMemoryState ?: loadSyncStateFromDisk()
        // Whether the initial full export (whole history) has ever completed for
        // this user. While false, apps can show a "keep the app open" hint.
        val initialExportDone = hasCompletedInitialSync()
        return if (state != null) {
            mapOf(
                "hasResumableSession" to state.hasProgress,
                "sentCount" to state.totalSentCount,
                "completedTypes" to state.completedTypes.size,
                "isFullExport" to state.fullExport,
                "initialExportDone" to initialExportDone,
                "isSyncing" to SyncRunGate.isRunning,
                "createdAt" to dateFormatter.format(java.time.Instant.ofEpochMilli(state.createdAt))
            )
        } else {
            mapOf(
                "hasResumableSession" to false,
                "sentCount" to 0,
                "completedTypes" to 0,
                "isFullExport" to false,
                "initialExportDone" to initialExportDone,
                "isSyncing" to SyncRunGate.isRunning,
                "createdAt" to null
            )
        }
    }

    fun hasResumableSyncSession(): Boolean {
        return (inMemoryState ?: loadSyncStateFromDisk())?.hasProgress == true
    }

    fun clearSyncSession() {
        clearSyncSessionInternal()
        logger("Cleared sync state")
    }

    /** Gutsy fork (OW-P3, #16 hook): this member's recent attempts, newest first. Not bridged yet (SyncAttempts.kt). */
    fun getSyncAttempts(): List<Map<String, Any?>> = attempts.list(userKey()).map { it.toMap() }

    /** Gutsy fork (OW-P3): a sign-in or sign-out inherits no backoff window, attempt history or scheduled retry. */
    fun clearRetryState() {
        backoff.clear()
        attempts.clear()
        try { scheduler.cancel() } catch (_: Exception) { logger("Retry cancellation unconfirmed") }
    }
}
