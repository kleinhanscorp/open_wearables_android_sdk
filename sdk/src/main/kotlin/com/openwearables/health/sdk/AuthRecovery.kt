package com.openwearables.health.sdk

import android.util.Base64
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.json.JSONObject
import java.io.IOException

/**
 * Gutsy fork — AUTH SELF-HEALING WITHOUT A HOST RUNTIME (the Android twin of the iOS
 * `Internal/AuthRecovery.swift`).
 *
 * Open Wearables rotates refresh tokens strictly: `POST /token/refresh` revokes the presented
 * token and mints a new one, with no grace window. The moment the server answers 200, the old
 * token is dead. If the process dies before the new pair is persisted — or two managers refresh
 * the same token concurrently — the device holds a revoked token and every later request is a
 * 401, forever (the-momentum/open-wearables#1379).
 *
 * Three layers, each independent of the host app:
 *   1. **One refresh at a time, process-wide.** `HealthSyncWorker` builds its own `SyncManager`,
 *      so a per-manager lock protects nothing. The mutex lives in the companion.
 *   2. **Double-check before despair.** A rejected refresh re-reads storage; if another writer
 *      already installed a newer pair, that pair is adopted.
 *   3. **Native renewal.** A long-lived, non-rotating renewal credential handed over by the host
 *      mints a fresh pair at a host-owned endpoint — no JS, no member action.
 * Plus a persisted verdict (`ok` / `recovering` / `reauth_required`) the host reads at launch.
 */
internal class AuthRecovery(
    private val storage: SecureStorage,
    private val httpClient: OkHttpClient,
    private val logger: (String) -> Unit,
    private val onAuthStateChanged: ((String) -> Unit)? = null,
    private val onAuthRecovered: ((String) -> Unit)? = null,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO
) {
    enum class Result { SUCCESS, AUTH_FAILURE, NETWORK_ERROR }

    private enum class RenewalOutcome { RENEWED, REJECTED, TRANSIENT, UNAVAILABLE }

    companion object {
        const val STATE_OK = "ok"
        const val STATE_RECOVERING = "recovering"
        const val STATE_REAUTH_REQUIRED = "reauth_required"

        /** Minimum spacing between renewal calls. A refresh storm must never become a renewal storm. */
        private const val RENEWAL_MIN_GAP_MS = 60_000L
        /** Consecutive rejections after which the SDK stops asking and reports `reauth_required`. */
        private const val RENEWAL_MAX_CONSECUTIVE_REJECTIONS = 2
        /** Access tokens expiring inside this window are refreshed proactively by `ensureFreshSession`. */
        private const val FRESHNESS_HORIZON_MS = 5 * 60_000L

        /** Process-wide single-flight for refresh + recovery. */
        private val refreshMutex = Mutex()

        private val JSON_MEDIA = "application/json".toMediaType()

        private fun iso(ms: Long): String? =
            if (ms > 0L) java.time.Instant.ofEpochMilli(ms).toString() else null
    }

    // ───────────────────────────── Public surface (via the SDK) ─────────────────────────────

    /** Refresh under the process-wide lock, walking the recovery ladder on rejection. */
    suspend fun refreshTokens(): Result = refreshMutex.withLock {
        withContext(ioDispatcher) { refreshLocked() }
    }

    /**
     * Prove — or repair — the session now. Returns "ok" · "refreshed" · "recovered" ·
     * "reauth_required" · "network_error".
     */
    suspend fun ensureFreshSession(): String {
        val state = storage.getAuthState()
        val exp = accessTokenExpiryMs()
        if (state != STATE_REAUTH_REQUIRED && exp != null && exp - System.currentTimeMillis() > FRESHNESS_HORIZON_MS) {
            return "ok"
        }
        val recoveredBefore = storage.getLastRecoveredAt()
        return when (refreshTokens()) {
            Result.SUCCESS -> {
                val recoveredAfter = storage.getLastRecoveredAt()
                if (recoveredAfter > 0L && recoveredAfter != recoveredBefore) "recovered" else "refreshed"
            }
            Result.AUTH_FAILURE -> "reauth_required"
            Result.NETWORK_ERROR -> "network_error"
        }
    }

    /** Snapshot for bridges. No network. */
    fun getAuthState(): Map<String, Any?> = mapOf(
        "state" to storage.getAuthState(),
        "lastAuthErrorAt" to iso(storage.getLastAuthErrorAt()),
        "lastRecoveredAt" to iso(storage.getLastRecoveredAt()),
        "hasRenewalCredential" to storage.hasRenewalCredential(),
        "renewalFailures" to storage.getRenewalFailures(),
        "accessTokenExpiresAt" to iso(accessTokenExpiryMs() ?: 0L),
        "hasSession" to storage.hasSession()
    )

    /** Install (or replace) the renewal credential; lifts a `reauth_required` verdict. */
    fun setRenewalCredential(url: String, token: String) {
        storage.saveRenewalCredential(url, token)
        storage.setRenewalFailures(0)
        storage.setLastRenewalAttemptAt(0L)
        if (storage.getAuthState() == STATE_REAUTH_REQUIRED && storage.hasAuth) {
            transition(STATE_OK)
        }
        logger("Renewal credential installed")
    }

    fun clearRenewalCredential() {
        storage.clearRenewalCredential()
        logger("Renewal credential cleared")
    }

    /** A good pair is installed. `via`: "refresh" · "double-check" · "renewal" · "external". */
    fun markAuthOk(via: String) {
        val wasBroken = storage.getAuthState() != STATE_OK
        if (via == "renewal" || via == "external" || via == "double-check") {
            storage.setLastRecoveredAt(System.currentTimeMillis())
        }
        transition(STATE_OK)
        if (wasBroken || via == "renewal") {
            onAuthRecovered?.invoke(via)
        }
    }

    fun markReauthRequired() {
        storage.setLastAuthErrorAt(System.currentTimeMillis())
        transition(STATE_REAUTH_REQUIRED)
    }

    /** RFC 7009 revoke of the refresh token we are about to forget. Fire-and-forget. */
    fun revokeRefreshTokenBestEffort() {
        val base = storage.apiBaseUrl ?: return
        val refreshToken = storage.getRefreshToken() ?: return
        try {
            val body = JSONObject().put("refresh_token", refreshToken).toString().toRequestBody(JSON_MEDIA)
            val request = Request.Builder()
                .url("$base/token/revoke")
                .post(body)
                .header("Content-Type", "application/json")
                .build()
            httpClient.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    logger("Refresh token revoke failed: ${e.message}")
                }

                override fun onResponse(call: Call, response: Response) {
                    response.use { logger("Refresh token revoke: HTTP ${it.code}") }
                }
            })
        } catch (e: Exception) {
            logger("Refresh token revoke skipped: ${e.message}")
        }
    }

    // ───────────────────────────── The ladder (runs under refreshMutex) ─────────────────────

    private fun refreshLocked(): Result {
        val base = storage.apiBaseUrl
        if (base == null) {
            logger("Token refresh failed: not configured")
            return Result.NETWORK_ERROR
        }
        val presented = storage.getRefreshToken()
        if (presented == null) {
            // A lost write or a partial sign-in. Not necessarily fatal: renewal can still mint a pair.
            logger("Token refresh: no refresh token - trying recovery")
            return resolveRefreshRejection(null)
        }

        return try {
            val body = JSONObject().put("refresh_token", presented).toString().toRequestBody(JSON_MEDIA)
            val request = Request.Builder()
                .url("$base/token/refresh")
                .post(body)
                .header("Content-Type", "application/json")
                .build()
            httpClient.newCall(request).execute().use { response ->
                val code = response.code
                val text = response.body?.string()
                when {
                    code in 401..403 -> {
                        logger("Token refresh rejected: HTTP $code - starting recovery")
                        resolveRefreshRejection(presented)
                    }
                    !response.isSuccessful || text == null -> {
                        logger("Token refresh failed: HTTP $code")
                        Result.NETWORK_ERROR
                    }
                    else -> {
                        val json = JSONObject(text)
                        val access = json.optString("access_token", "")
                        if (access.isEmpty()) {
                            logger("Token refresh failed: invalid response body")
                            Result.NETWORK_ERROR
                        } else {
                            val refresh = if (json.isNull("refresh_token")) null else json.optString("refresh_token", "")
                            // The server has ALREADY revoked `presented`. Persist the new pair
                            // (refresh token first, verified) BEFORE anyone retries with it.
                            if (!storage.updateTokens(access, refresh?.takeIf { it.isNotEmpty() })) {
                                logger("Token refresh: HTTP $code but the pair did not persist - treating as transient")
                                Result.NETWORK_ERROR
                            } else {
                                logger("Token refresh: HTTP $code")
                                markAuthOk("refresh")
                                Result.SUCCESS
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger("Token refresh failed: ${e.javaClass.simpleName}: ${e.message}")
            Result.NETWORK_ERROR
        }
    }

    private fun resolveRefreshRejection(presented: String?): Result {
        // 1. Did another writer already install a newer refresh token? Then the rejection was for
        //    a stale value and the session is fine.
        val current = storage.getRefreshToken()
        if (current != null && current != presented) {
            logger("Refresh rejected for a superseded token - a newer pair is already installed")
            markAuthOk("double-check")
            return Result.SUCCESS
        }

        // 2. Native renewal.
        transition(STATE_RECOVERING)
        return when (attemptRenewal()) {
            RenewalOutcome.RENEWED -> Result.SUCCESS
            RenewalOutcome.TRANSIENT -> Result.NETWORK_ERROR // stay `recovering`; next trigger retries after the backoff
            RenewalOutcome.REJECTED, RenewalOutcome.UNAVAILABLE -> {
                markReauthRequired()
                Result.AUTH_FAILURE
            }
        }
    }

    private fun attemptRenewal(): RenewalOutcome {
        val url = storage.getRenewalUrl()
        val token = storage.getRenewalToken()
        if (url == null || token == null) {
            logger("Renewal unavailable: no renewal credential installed")
            return RenewalOutcome.UNAVAILABLE
        }
        val failures = storage.getRenewalFailures()
        if (failures >= RENEWAL_MAX_CONSECUTIVE_REJECTIONS) {
            logger("Renewal exhausted: credential rejected ${failures}x")
            return RenewalOutcome.REJECTED
        }
        val last = storage.getLastRenewalAttemptAt()
        val sinceLast = System.currentTimeMillis() - last
        if (last > 0L && sinceLast < RENEWAL_MIN_GAP_MS) {
            logger("Renewal skipped: attempted ${sinceLast / 1000}s ago")
            return RenewalOutcome.TRANSIENT
        }
        storage.setLastRenewalAttemptAt(System.currentTimeMillis())

        logger("Renewing SDK credentials via host...")
        return try {
            val payload = JSONObject()
                .put("renewal_token", token)
                .put("platform", "android")
                .put("sdk_version", SyncDefaults.SDK_VERSION)
                .toString()
                .toRequestBody(JSON_MEDIA)
            val request = Request.Builder()
                .url(url)
                .post(payload)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build()
            httpClient.newCall(request).execute().use { response ->
                val code = response.code
                val text = response.body?.string()
                when {
                    code == 401 || code == 403 || code == 410 -> {
                        storage.setRenewalFailures(failures + 1)
                        logger("Renewal rejected: HTTP $code (${failures + 1}x)")
                        RenewalOutcome.REJECTED
                    }
                    !response.isSuccessful || text == null -> {
                        logger("Renewal failed: HTTP $code")
                        RenewalOutcome.TRANSIENT
                    }
                    else -> {
                        val json = JSONObject(text)
                        val access = json.optString("access_token", "")
                        val refresh = json.optString("refresh_token", "")
                        if (access.isEmpty() || refresh.isEmpty()) {
                            logger("Renewal failed: incomplete pair")
                            RenewalOutcome.TRANSIENT
                        } else if (!storage.updateTokens(access, refresh)) {
                            logger("Renewal succeeded but the pair could not be persisted")
                            RenewalOutcome.TRANSIENT
                        } else {
                            storage.setRenewalFailures(0)
                            logger("Renewal: HTTP $code - fresh pair installed")
                            markAuthOk("renewal")
                            RenewalOutcome.RENEWED
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger("Renewal failed: ${e.javaClass.simpleName}: ${e.message}")
            RenewalOutcome.TRANSIENT
        }
    }

    // ───────────────────────────── The ledger ─────────────────────────────

    private fun transition(next: String) {
        val previous = storage.getAuthState()
        if (previous == next) return
        storage.saveAuthState(next)
        logger("Auth state: $previous → $next")
        onAuthStateChanged?.invoke(next)
    }

    /** `exp` (epoch ms) of the stored access token, decoded locally. null when absent/unparseable. */
    fun accessTokenExpiryMs(): Long? {
        val token = storage.getAccessToken() ?: return null
        val raw = if (token.startsWith("Bearer ")) token.substring(7) else token
        val parts = raw.split('.')
        if (parts.size < 2) return null
        return try {
            val decoded = Base64.decode(parts[1], Base64.URL_SAFE or Base64.NO_PADDING or Base64.NO_WRAP)
            val exp = JSONObject(String(decoded, Charsets.UTF_8)).optLong("exp", 0L)
            if (exp > 0L) exp * 1000L else null
        } catch (e: Exception) {
            null
        }
    }
}
