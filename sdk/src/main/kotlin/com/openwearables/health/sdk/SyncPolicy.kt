package com.openwearables.health.sdk

import kotlin.random.Random

/**
 * Gutsy fork (OW-P3, GUT-217 #6) — the decisions one sync run makes, kept free of Android types so
 * the JVM tests exercise the real rules: which tracked types a run may read, how a failure is
 * classified, how long to wait before the next attempt, and how an exhausted page session moves on.
 */

/** Per-type status a run records. Only [COMPLETED] advances a type; every other status leaves it pending. */
object TypeStatus {
    const val READABLE = "readable"
    /** This provider has no reader for the id (e.g. the 19 iOS-only ids on Health Connect). */
    const val UNSUPPORTED = "unsupported"
    /** Another tracked id reads the same records (distance, blood pressure, water); read once. */
    const val ALIAS = "alias"
    /** The read permission is absent at run start. */
    const val NOT_GRANTED = "not_granted"
    /** The read was refused mid-run (permission revoked after the grant check). */
    const val READ_DENIED = "read_denied"
    /** A transient read error; the type resumes on the next attempt. */
    const val READ_FAILED = "read_failed"
    /** [READ_FAILED] [SyncDefaults.TYPE_READ_FAILURE_LIMIT] times running; parked so the session can finish. */
    const val READ_FAILED_REPEATEDLY = "read_failed_repeatedly"
    /** The page session hit its budget and could not roll over (no accepted progress to commit). */
    const val PAGE_BUDGET_STALLED = "page_budget_stalled"
    const val SAMPLE_BUDGET_EXCEEDED = "sample_budget_exceeded"
    const val PROVIDER_UNAVAILABLE = "provider_unavailable"
    const val COMPLETED = "completed"
    /** Readable, but the run stopped before the type finished (upload failure, retry pending). */
    const val PENDING = "pending"
}

/** Which tracked ids this run reads, and why each of the others is skipped. */
data class TypeReadability(
    val readable: List<String>,
    val statuses: Map<String, String>,
    /** The grant query itself failed (provider busy): nothing is known, so the run retries. */
    val transientFailure: Boolean = false,
    /** Health Connect's background-read grant, when the provider knows it. */
    val backgroundReadGranted: Boolean? = null
) {
    val providerUnavailable: Boolean
        get() = readable.isEmpty() && statuses.values.any { it == TypeStatus.PROVIDER_UNAVAILABLE }

    companion object {
        fun all(typeIds: Collection<String>) =
            TypeReadability(typeIds.distinct(), typeIds.distinct().associateWith { TypeStatus.READABLE })
        fun unavailable(typeIds: Collection<String>) =
            TypeReadability(emptyList(), typeIds.distinct().associateWith { TypeStatus.PROVIDER_UNAVAILABLE })
    }
}

/** How a run ended. Only [RETRY] schedules another attempt; [TERMINAL] waits for the host or the next period. */
enum class SyncOutcome { COMPLETED, PARTIAL, RETRY, TERMINAL, BACKED_OFF, BUSY, SUPERSEDED, CANCELLED }

/** What started a run. Foreground triggers may cut a soft backoff short; none may beat a server's Retry-After. */
object SyncTrigger {
    const val FOREGROUND = "foreground"
    const val RESUME = "resume"
    const val RESET = "reset"
    const val PERIODIC = "periodic"
    const val EXPEDITED = "expedited"
    const val RETRY = "retry"

    internal fun isForeground(trigger: String) = trigger == FOREGROUND || trigger == RESUME || trigger == RESET
}

/** The connection this run started under was replaced; its progress stays on disk for its owner. */
internal class SyncSupersededException : IllegalStateException("Sync connection superseded; progress retained")

internal object SyncPolicy {
    /** Of two ids that read the same records, the one the canonical Android list keeps (Codex 6B). */
    val PREFERRED_ALIASES = setOf("distanceWalkingRunning", "bloodPressure", "dietaryWater")

    /**
     * Every run re-intersects what is tracked, what the provider can read and what is granted now.
     * A type revoked since Connect is skipped with a status instead of failing the combined round,
     * and ids that share a reader are read once, through the preferred id.
     */
    fun resolveReadable(
        tracked: List<String>,
        readerOf: (String) -> String?,
        permissionOf: (String) -> String?,
        granted: Set<String>,
        preferred: Set<String> = PREFERRED_ALIASES
    ): TypeReadability {
        val statuses = linkedMapOf<String, String>()
        val byReader = linkedMapOf<String, MutableList<String>>()
        for (type in tracked.distinct()) {
            val reader = readerOf(type)
            if (reader == null) statuses[type] = TypeStatus.UNSUPPORTED
            else byReader.getOrPut(reader) { mutableListOf() }.add(type)
        }
        val readable = mutableListOf<String>()
        for (ids in byReader.values) {
            val keeper = ids.firstOrNull { it in preferred } ?: ids.first()
            for (alias in ids) if (alias != keeper) statuses[alias] = TypeStatus.ALIAS
            val permission = permissionOf(keeper)
            if (permission != null && permission in granted) {
                statuses[keeper] = TypeStatus.READABLE
                readable.add(keeper)
            } else {
                statuses[keeper] = TypeStatus.NOT_GRANTED
            }
        }
        return TypeReadability(readable, statuses)
    }

    /** A wanted type the run could not finish. No reader / an alias is the provider's shape, not a shortfall. */
    fun isShortfall(status: String): Boolean =
        status != TypeStatus.READABLE && status != TypeStatus.COMPLETED &&
            status != TypeStatus.UNSUPPORTED && status != TypeStatus.ALIAS

    enum class FailureKind { TRANSIENT, AUTH, REJECTED }

    /** `null` = no HTTP answer at all (offline, timeout, reset), which is transient. */
    fun classifyHttp(status: Int?): FailureKind = when {
        status == null -> FailureKind.TRANSIENT
        status == 408 || status == 425 || status == 429 || status in 500..599 -> FailureKind.TRANSIENT
        status == 401 || status == 403 -> FailureKind.AUTH
        else -> FailureKind.REJECTED
    }

    /** Retry-After as delta-seconds or an HTTP date; `null` when absent or unreadable. */
    fun parseRetryAfterSeconds(value: String?, nowMs: Long): Long? {
        val raw = value?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        raw.toLongOrNull()?.let { return it.coerceAtLeast(0L) }
        return try {
            val at = java.time.ZonedDateTime.parse(raw, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
                .toInstant().toEpochMilli()
            ((at - nowMs) / 1000L).coerceAtLeast(0L)
        } catch (_: Exception) {
            null
        }
    }

    const val BACKOFF_BASE_MS = 60_000L
    const val BACKOFF_CAP_MS = 6 * 3_600_000L
    const val RETRY_AFTER_CAP_MS = 6 * 3_600_000L
    /** A failure this long after the last window opens a fresh streak instead of extending the old one. */
    const val BACKOFF_STREAK_RESET_MS = 24 * 3_600_000L

    /** Equal-jitter exponential delay for the n-th consecutive failure, never shorter than Retry-After. */
    fun backoffDelayMs(consecutiveFailures: Int, retryAfterSec: Long?, random: Random = Random.Default): Long {
        val n = consecutiveFailures.coerceIn(1, 30)
        val exponential = (BACKOFF_BASE_MS shl minOf(n - 1, 20)).coerceAtMost(BACKOFF_CAP_MS)
        val jittered = exponential / 2 + random.nextLong(exponential / 2 + 1)
        val floor = retryAfterSec?.let { (it * 1000L).coerceIn(0L, RETRY_AFTER_CAP_MS) } ?: 0L
        return maxOf(jittered, floor)
    }

    /**
     * Bounds for a page session that reached its budget. Ascending (incremental) work restarts at
     * its accepted watermark — the point a completed session would have committed as the anchor;
     * descending (historical) work narrows its upper bound to just above the oldest accepted start.
     * `null` when the session cannot move, so the caller parks the type instead of looping.
     */
    fun rolloverBounds(startMs: Long, endMs: Long, ascending: Boolean, watermarkMs: Long?, oldestStartMs: Long?): Pair<Long, Long>? =
        if (ascending) watermarkMs?.takeIf { it > startMs && it < endMs }?.let { it to endMs }
        else oldestStartMs?.let { it + 1 }?.takeIf { it > startMs && it < endMs }?.let { startMs to it }

    /** A record open longer than this before the query's end is not waited for (a 25-hour DST day fits). */
    const val OPEN_RECORD_HOLD_MAX_MS = 36 * 3_600_000L

    /**
     * Gutsy fork (GUT-217): the earliest start among records still open at the query's end, given
     * as (start, end) spans. Samsung Health writes a day's steps total from local midnight to the
     * NEXT midnight and keeps it growing; `null` when every record has closed. A record begun more
     * than [OPEN_RECORD_HOLD_MAX_MS] earlier (a far-future end) never holds a type's reads open.
     */
    fun openStartMs(spans: List<Pair<Long, Long>>, queryEndMs: Long): Long? =
        spans.filter { it.second > queryEndMs && queryEndMs - it.first <= OPEN_RECORD_HOLD_MAX_MS }.minOfOrNull { it.first }

    /**
     * Gutsy fork (GUT-217): the anchor a type may commit. Health Connect selects a read by record
     * START, inclusive, while the watermark is the newest record END; a record still open would put
     * the anchor in the future, where every later window is empty and `TimeRangeFilter.between`
     * throws before anything is sent. The anchor never passes the query's end, nor the start of a
     * record still open there, so that record is read again until it closes; OW upserts the re-sent
     * row (data source, series type, start) and keeps the newer value.
     */
    fun committableAnchor(watermarkMs: Long?, queryEndMs: Long, openStartMs: Long?): Long? =
        watermarkMs?.let { minOf(it, queryEndMs, openStartMs ?: it) }

    /**
     * Gutsy fork (GUT-217): a stored anchor beyond now came from a fork before [committableAnchor],
     * from a record that was still open. Its start is unknown; the known producer is a local-day
     * total, so the anchor heals to the start of the local day and that day is read once more.
     */
    fun healedAnchor(anchorMs: Long, nowMs: Long, zone: java.time.ZoneId): Long =
        if (anchorMs <= nowMs) anchorMs
        else java.time.Instant.ofEpochMilli(nowMs).atZone(zone).toLocalDate().atStartOfDay(zone).toInstant().toEpochMilli()
}
