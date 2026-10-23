package com.openwearables.health.sdk

import android.content.SharedPreferences
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/**
 * Gutsy fork (OW-P3, GUT-217 #16 hook) — what each sync attempt reached and why it stopped:
 * stage, times, counts, an error code and the per-type statuses. No health values, tokens or
 * endpoints are recorded.
 *
 * Deliberately internal. OW-P2-DESIGN fixes the shape the host reads; when it lands, [toMap] is the
 * one place that changes, plus a single `getSyncAttempts` line in each wrapper bridge.
 */
@Serializable
internal data class SyncAttempt(
    val id: String,
    val ownerKey: String,
    val trigger: String,
    val startedAt: Long,
    var endedAt: Long? = null,
    var stage: String = SyncStage.START,
    var outcome: String? = null,
    var errorCode: String? = null,
    var httpStatus: Int? = null,
    var retryAfterSec: Long? = null,
    var nextAttemptAt: Long? = null,
    var trackedCount: Int = 0,
    var readableCount: Int = 0,
    var backgroundReadGranted: Boolean? = null,
    var rounds: Int = 0,
    var sentItems: Int = 0,
    var typeStatuses: MutableMap<String, String> = mutableMapOf()
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "id" to id, "trigger" to trigger, "startedAt" to startedAt, "endedAt" to endedAt,
        "stage" to stage, "outcome" to outcome, "errorCode" to errorCode, "httpStatus" to httpStatus,
        "retryAfterSec" to retryAfterSec, "nextAttemptAt" to nextAttemptAt,
        "trackedCount" to trackedCount, "readableCount" to readableCount,
        "backgroundReadGranted" to backgroundReadGranted, "rounds" to rounds, "sentItems" to sentItems,
        "typeStatuses" to typeStatuses.toMap()
    )
}

/** The furthest point an attempt reached, in order. */
internal object SyncStage {
    const val START = "start"
    const val AUTH = "auth"
    const val GATE = "backoff_gate"
    const val TYPES = "types"
    const val LEASE = "lease"
    const val READ = "read"
    const val UPLOAD = "upload"
    const val COMMIT = "commit"
    const val DONE = "done"
}

/** The last [max] attempts on this device, newest first, read back only for the member who ran them. */
internal class SyncAttemptLog(private val prefs: SharedPreferences, private val max: Int = SyncDefaults.SYNC_ATTEMPT_HISTORY) {
    private val json = Json { ignoreUnknownKeys = true }
    private val lock = Any()

    private fun load(): List<SyncAttempt> = try {
        prefs.getString(StorageKeys.KEY_SYNC_ATTEMPTS, null)?.let { json.decodeFromString<List<SyncAttempt>>(it) } ?: emptyList()
    } catch (_: Exception) {
        emptyList()
    }

    /** Written at the start too, so a run killed mid-way still shows where it stopped. */
    fun save(attempt: SyncAttempt) = synchronized(lock) {
        val next = (listOf(attempt.copy(typeStatuses = attempt.typeStatuses.toMutableMap())) + load().filter { it.id != attempt.id }).take(max)
        prefs.edit().putString(StorageKeys.KEY_SYNC_ATTEMPTS, json.encodeToString(next)).apply()
    }

    fun list(ownerKey: String): List<SyncAttempt> = synchronized(lock) { load().filter { it.ownerKey == ownerKey } }

    fun clear() = synchronized(lock) {
        prefs.edit().remove(StorageKeys.KEY_SYNC_ATTEMPTS).apply()
    }
}
