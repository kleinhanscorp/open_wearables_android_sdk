package com.openwearables.health.sdk

import android.app.Activity
import android.content.Context
import android.content.SharedPreferences
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import java.io.File
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/** Map-backed SharedPreferences: the sync prefs (anchors, retry window, attempts) the real run writes. */
internal class FakePrefs : SharedPreferences {
    val values: MutableMap<String, Any?> = Collections.synchronizedMap(linkedMapOf())

    override fun getAll(): MutableMap<String, *> = synchronized(values) { values.toMutableMap() }
    override fun getString(key: String?, defValue: String?): String? = values[key] as? String ?: defValue
    @Suppress("UNCHECKED_CAST")
    override fun getStringSet(key: String?, defValues: MutableSet<String>?): MutableSet<String>? =
        (values[key] as? Set<String>)?.toMutableSet() ?: defValues
    override fun getInt(key: String?, defValue: Int): Int = values[key] as? Int ?: defValue
    override fun getLong(key: String?, defValue: Long): Long = values[key] as? Long ?: defValue
    override fun getFloat(key: String?, defValue: Float): Float = values[key] as? Float ?: defValue
    override fun getBoolean(key: String?, defValue: Boolean): Boolean = values[key] as? Boolean ?: defValue
    override fun contains(key: String?): Boolean = values.containsKey(key)
    override fun edit(): SharedPreferences.Editor = Editor()
    override fun registerOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}
    override fun unregisterOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}

    private inner class Editor : SharedPreferences.Editor {
        private val pending = linkedMapOf<String, Any?>()
        private val removed = mutableSetOf<String>()
        private var clearAll = false
        private fun put(key: String?, value: Any?) = apply { pending[key!!] = value }
        override fun putString(key: String?, value: String?) = put(key, value)
        override fun putStringSet(key: String?, values: MutableSet<String>?) = put(key, values?.toSet())
        override fun putInt(key: String?, value: Int) = put(key, value)
        override fun putLong(key: String?, value: Long) = put(key, value)
        override fun putFloat(key: String?, value: Float) = put(key, value)
        override fun putBoolean(key: String?, value: Boolean) = put(key, value)
        override fun remove(key: String?) = apply { removed.add(key!!) }
        override fun clear() = apply { clearAll = true }
        override fun commit(): Boolean {
            synchronized(values) {
                if (clearAll) values.clear()
                removed.forEach { values.remove(it) }
                pending.forEach { (k, v) -> if (v == null) values.remove(k) else values[k] = v }
            }
            return true
        }
        override fun apply() { commit() }
    }
}

internal class RecordingScheduler : RetryScheduler {
    val scheduled = Collections.synchronizedList(mutableListOf<Pair<Long, Boolean>>())
    val cancels = AtomicInteger(0)
    override fun schedule(delayMs: Long, host: String, customSyncUrl: String?, replaceRunning: Boolean) {
        scheduled.add(delayMs to replaceRunning)
    }
    override fun cancel() { cancels.incrementAndGet() }
}

internal object TestDispatchers : DispatcherProvider {
    override val main: CoroutineDispatcher = Dispatchers.Unconfined
    override val io: CoroutineDispatcher = Dispatchers.IO
    override val default: CoroutineDispatcher = Dispatchers.Default
}

/** An interval record with a stable id (GUT-217), e.g. Samsung Health's midnight-to-midnight steps total. */
internal data class Span(val id: String, val startMs: Long, val endMs: Long, val value: Double = 1.0)

/**
 * A native-paging provider over synthetic timestamps. Pages honour the query's bounds and page
 * size like Health Connect (selected by start, inclusive), so rollover and resume behave as on a
 * device, and an empty or inverted window throws as `TimeRangeFilter.between` does.
 * Grants and readers are explicit; [aliasReader] maps ids that share one reader.
 */
internal class FakeProvider(
    tracked: List<String>,
    @Volatile var granted: Set<String>,
    private val readers: Set<String>,
    private val aliasReader: Map<String, String> = emptyMap()
) : HealthDataProvider {
    override val providerId = ProviderIds.GOOGLE
    override val providerName = "Fake Health Connect"
    /** false = the Samsung Health path: [readData] from a since-cursor instead of native pages. */
    @Volatile var nativePages = true
    override val supportsNativePages: Boolean get() = nativePages

    private var trackedIds = tracked.toSet()
    val data: MutableMap<String, List<Long>> = Collections.synchronizedMap(mutableMapOf())
    /** Interval records per type; a type listed here is read from its spans, not from [data]. */
    val spans: MutableMap<String, List<Span>> = Collections.synchronizedMap(mutableMapOf())
    /** Every span id handed to the run, in order (upload bodies are empty under the JsonWriter stub). */
    val served: MutableList<String> = Collections.synchronizedList(mutableListOf())
    val queries: MutableList<Pair<String, ProviderPageQuery>> = Collections.synchronizedList(mutableListOf())
    val failures: MutableMap<String, () -> Throwable> = Collections.synchronizedMap(mutableMapOf())
    val reads: MutableList<String> = Collections.synchronizedList(mutableListOf())
    @Volatile var hold: CompletableDeferred<Unit>? = null
    val entered = CompletableDeferred<Unit>()
    /** Claim every tracked id readable, as an old persisted list met by a provider that over-reports would. */
    @Volatile var claimAllReadable = false
    private val active = AtomicInteger(0)
    @Volatile var maxActive = 0

    private fun reader(id: String) = aliasReader[id] ?: id

    override fun isAvailable() = true
    override suspend fun connect() = true
    override fun disconnect() {}
    override fun setActivity(activity: Activity?) {}
    override fun setTrackedTypes(typeIds: List<String>) { trackedIds = typeIds.toSet() }
    override fun getTrackedTypes(): Set<String> = trackedIds
    override suspend fun requestAuthorization(typeIds: List<String>) = true
    override suspend fun readData(typeId: String, sinceTimestamp: Long?, limit: Int): ProviderReadResult {
        check(!nativePages) { "native pages only" }
        reads.add(typeId)
        // Samsung Health reads (since, now]; a cursor at or past now is an inverted window.
        require(sinceTimestamp == null || sinceTimestamp + 1 < System.currentTimeMillis()) { "end time needs be after start time" }
        val page = (spans[typeId] ?: emptyList()).filter { sinceTimestamp == null || it.startMs > sinceTimestamp }
            .sortedBy { it.startMs }.take(limit)
        page.forEach { served.add(it.id) }
        return ProviderReadResult(UnifiedHealthData(records = page.map { spanRecord(typeId, it) }), page.maxOfOrNull { it.endMs })
    }

    override suspend fun resolveReadableTypes(typeIds: Collection<String>): TypeReadability =
        if (claimAllReadable) TypeReadability.all(typeIds) else SyncPolicy.resolveReadable(typeIds.toList(), { if (it in readers) reader(it) else null }, { "read:${reader(it)}" },
            granted.map { "read:${reader(it)}" }.toSet())

    override suspend fun readPage(typeId: String, query: ProviderPageQuery, pageToken: String?): ProviderReadResult {
        val now = active.incrementAndGet()
        synchronized(this) { maxActive = maxOf(maxActive, now) }
        try {
            reads.add(typeId)
            queries.add(typeId to query)
            hold?.let { entered.complete(Unit); it.await() }
            failures[typeId]?.let { throw it() }
            if (typeId !in readers) return ProviderReadResult(UnifiedHealthData(), null, null)
            require(query.startMs < query.endMs) { "end time needs be after start time" }
            spans[typeId]?.let { return spanPage(typeId, it, query, pageToken) }
            val inRange = (data[typeId] ?: emptyList()).filter { it >= query.startMs && it < query.endMs }
                .let { if (query.ascending) it.sorted() else it.sortedDescending() }
            val offset = pageToken?.toInt() ?: 0
            val page = inRange.drop(offset).take(query.pageSize)
            val next = (offset + page.size).takeIf { it < inRange.size }?.toString()
            val records = page.mapIndexed { i, ts -> record(typeId, offset + i, ts) }
            return ProviderReadResult(UnifiedHealthData(records = records), page.maxOrNull(), page.minOrNull(),
                page.size, next, true, page.minOrNull())
        } finally {
            active.decrementAndGet()
        }
    }

    /** Like HealthConnectManager's page: selected by start, watermark = newest end, open starts reported. */
    private fun spanPage(typeId: String, all: List<Span>, query: ProviderPageQuery, pageToken: String?): ProviderReadResult {
        val inRange = all.filter { it.startMs >= query.startMs && it.startMs < query.endMs }
            .let { if (query.ascending) it.sortedBy { s -> s.startMs } else it.sortedByDescending { s -> s.startMs } }
        val offset = pageToken?.toInt() ?: 0
        val page = inRange.drop(offset).take(query.pageSize)
        val next = (offset + page.size).takeIf { it < inRange.size }?.toString()
        page.forEach { served.add(it.id) }
        return ProviderReadResult(UnifiedHealthData(records = page.map { spanRecord(typeId, it) }), page.maxOfOrNull { it.endMs },
            if (query.ascending) null else page.lastOrNull()?.endMs, page.size, next, true, page.minOfOrNull { it.startMs },
            SyncPolicy.openStartMs(page.map { it.startMs to it.endMs }, query.endMs))
    }

    private fun spanRecord(type: String, span: Span) = UnifiedRecord(span.id, type.uppercase(),
        UnifiedTimestamp.fromEpochMs(span.startMs), UnifiedTimestamp.fromEpochMs(span.endMs), null,
        UnifiedSource("synthetic.app", null, null, null, null, null, null), span.value, "count", null, null)

    private fun record(type: String, index: Int, ts: Long): UnifiedRecord {
        val iso = UnifiedTimestamp.fromEpochMs(ts)
        return UnifiedRecord("$type-$index-$ts", type.uppercase(), iso, iso, null,
            UnifiedSource("synthetic.app", null, null, null, null, null, null), 1.0, "count", null, null)
    }
}

/** Routes requests by URL; every response is synthetic, nothing leaves the JVM. */
internal class FakeHttp {
    val calls: MutableList<String> = Collections.synchronizedList(mutableListOf())
    @Volatile var sync: (Int) -> Pair<Int, Map<String, String>> = { 202 to emptyMap() }
    @Volatile var lease: () -> Pair<Int, String> = { 200 to LEASE_OK }
    @Volatile var refresh: () -> Int = { 200 }
    @Volatile var renewal: () -> Int = { 401 }
    @Volatile var offline = false
    private val syncCount = AtomicInteger(0)

    val client: OkHttpClient = mockk<OkHttpClient>().also { client ->
        every { client.newCall(any()) } answers { FakeCall(firstArg()) }
    }

    private inner class FakeCall(private val request: Request) : okhttp3.Call {
        override fun request() = request
        override fun execute(): Response = route(request)
        override fun enqueue(responseCallback: okhttp3.Callback) {
            try { responseCallback.onResponse(this, route(request)) } catch (e: java.io.IOException) { responseCallback.onFailure(this, e) }
        }
        override fun cancel() {}
        override fun isExecuted() = false
        override fun isCanceled() = false
        override fun timeout(): okio.Timeout = okio.Timeout.NONE
        override fun clone(): okhttp3.Call = FakeCall(request)
    }

    fun syncPosts() = calls.count { it.endsWith("/sync") }
    fun leaseChecks() = calls.count { it == LEASE_CHECK }

    private fun route(request: Request): Response {
        // The lease check and the renewal share one URL; the lease check says validate_only.
        val sent = okio.Buffer().also { request.body?.writeTo(it) }.readUtf8()
        val url = request.url.toString().let { if (it == LEASE_URL && sent.contains("validate_only")) LEASE_CHECK else it }
        calls.add(url)
        if (offline && !url.endsWith("/logs")) throw java.io.IOException("synthetic: network unreachable")
        val (code, headers, body) = when {
            url == LEASE_CHECK -> lease().let { Triple(it.first, emptyMap(), it.second) }
            url.endsWith("/token/refresh") -> Triple(refresh(), emptyMap(), "{\"access_token\":\"fresh\",\"refresh_token\":\"fresh-r\"}")
            url == LEASE_URL -> Triple(renewal(), emptyMap(), "{}")
            url.endsWith("/sync") -> sync(syncCount.incrementAndGet()).let { Triple(it.first, it.second, "{}") }
            else -> Triple(200, emptyMap(), "{}")
        }
        return Response.Builder().request(request).protocol(Protocol.HTTP_1_1).code(code).message("synthetic")
            .apply { headers.forEach { (k, v) -> header(k, v) } }
            .body(body.toResponseBody("application/json".toMediaType())).build()
    }

    companion object {
        const val LEASE_URL = "https://lease.synthetic.invalid/renew"
        const val LEASE_CHECK = "$LEASE_URL#validate_only"
        const val LEASE_OK = "{\"lease_active\":true,\"connection_generation\":\"device-gen\",\"device_id\":\"device-1\"}"
    }
}

/** A connected member on a synthetic device; the owner generation is unique per test (AuthRecovery's lease cache is process-wide). */
internal class SyncHarness(
    val provider: FakeProvider,
    val prefs: FakePrefs = FakePrefs(),
    val http: FakeHttp = FakeHttp(),
    val scheduler: RecordingScheduler = RecordingScheduler(),
    val filesDir: File = java.nio.file.Files.createTempDirectory("ow-p3-sync").toFile()
) {
    @Volatile var owner = SecureStorage.ConnectionOwner("member-1", "gen-" + java.util.UUID.randomUUID())
    @Volatile var authState = AuthRecovery.STATE_OK
    val logs: MutableList<String> = Collections.synchronizedList(mutableListOf())
    private val ownerLock = Any()

    val storage: SecureStorage = mockk<SecureStorage>(relaxed = true).also { s ->
        every { s.connectionOwner() } answers { synchronized(ownerLock) { owner } }
        every { s.isCurrent(any()) } answers { synchronized(ownerLock) { firstArg<SecureStorage.ConnectionOwner>() == owner } }
        every { s.withConnection<Any?>(any(), any()) } answers {
            synchronized(ownerLock) { if (firstArg<SecureStorage.ConnectionOwner>() == owner) secondArg<() -> Any?>().invoke() else null }
        }
        every { s.getUserId() } answers { owner.userId }
        every { s.hasAuth } returns true
        every { s.isApiKeyAuth } returns false
        every { s.getAccessToken() } returns "access"
        every { s.authCredential } returns "access"
        every { s.getRefreshToken() } returns "refresh"
        every { s.apiBaseUrl } returns "https://ow.synthetic.invalid/api/v1"
        every { s.getAuthState() } answers { authState }
        every { s.deviceLeaseRejected() } returns false
        every { s.getSyncDaysBack() } returns 0
        every { s.getRenewalUrl() } returns FakeHttp.LEASE_URL
        every { s.getRenewalToken() } returns "renewal"
        every { s.deviceConnectionGeneration() } returns "device-gen"
        every { s.deviceId() } returns "device-1"
        every { s.getRenewalFailures() } returns 0
        every { s.getLastRenewalAttemptAt() } returns 0L
        every { s.updateTokens(any(), any()) } returns true
    }

    val context: Context = mockk<Context>(relaxed = true).also { c ->
        every { c.getSharedPreferences(any(), any()) } returns prefs
        every { c.filesDir } returns filesDir
    }

    fun manager(): SyncManager = SyncManager(context, storage, provider, TestDispatchers, { logs.add(it) },
        httpClient = http.client, retryScheduler = scheduler)

    /** The member has finished the initial export, so runs are incremental (ascending pages). */
    fun markInitialExportDone() = prefs.edit().putBoolean("fullDone.user.${owner.userId}", true).commit()

    fun anchors(): String = prefs.getString(StorageKeys.KEY_ANCHORS, null) ?: "{}"

    /** Anchors as SyncManager stores them (epoch ms as JSON numbers). */
    fun storeAnchors(vararg anchors: Pair<String, Long>) = prefs.edit().putString(StorageKeys.KEY_ANCHORS,
        Json.encodeToString(anchors.toMap().mapValues { it.value.toDouble() })).commit()

    fun anchorOf(type: String): Long? = Json.decodeFromString<Map<String, Double>>(anchors())[type]?.toLong()
}
