# Changelog

## Unreleased — Gutsy fork (`kleinhanscorp/open_wearables_android_sdk`), on top of 0.11.2

* **Refresh-rotation lockout closed** (upstream #1379). Token writes are now synchronous (`commit()`), **refresh-token-first and read-back-verified**; a rejected refresh **re-reads storage** and adopts a newer pair if a concurrent writer already installed one; refresh + recovery run under a **process-wide mutex** (`HealthSyncWorker` builds its own `SyncManager`, so a per-manager lock protected nothing).
* **Native renewal (JS-free self-heal)**: `setRenewalCredential(url, token)` installs a long-lived, non-rotating credential the SDK exchanges for a fresh pair when refresh is rejected — 60 s spacing, two-strike cutoff. `clearRenewalCredential()`, `hasRenewalCredential()`.
* **Sticky auth ledger**: `getAuthState()` (`ok` / `recovering` / `reauth_required` + timestamps), `authStateListener`, `authRecoveredListener`. `authErrorListener` fires only after the ladder is exhausted and the verdict is persisted.
* **`ensureFreshSession()`** (suspend): the host-safe session probe — proactive refresh inside 5 minutes of expiry, ladder on rejection.
* **Sign-out revokes** the refresh token server-side, best-effort (RFC 7009).
* Kept from the earlier fork commit: fully buffered upload body with `Content-Length` (OpenLiteSpeed rejects chunked transfer encoding).

## 0.11.2

* **New `getSyncStatus()` fields**: `initialExportDone` (Bool) and `isSyncing` (Bool) — allows apps to show progress UI during the initial historical export.

## 0.11.1

* **Fixed JVM signature clash**: removed the redundant `setLogLevel` setter that clashed with the `logLevel` property's generated JVM signature.

## 0.11.0

* **Public `setLogLevel(level)` method** added for parity with iOS. Convenience wrapper around the existing `logLevel` property, intended for cross-platform bridges (React Native, Flutter) and Java callers. The `logLevel` property remains available.
* **Fixed published Maven version**: the `:sdk` module publication was still declaring `0.9.0` in `build.gradle.kts` despite the `SDK_VERSION` constant being bumped. The published POM now matches the git tag and `SyncDefaults.SDK_VERSION`.

## 0.10.0

* **Sync telemetry**: new `/logs` endpoint integration for initial full sync diagnostics.
  - `historical_data_sync_start` event sent before the first payload with per-type record counts, time range, and device state.
  - `historical_data_type_sync_end` event sent per data type as each completes, with record count, duration, success status, and device state snapshot.
  - Device state includes battery level/state, thermal state, low power mode, and RAM usage.
  - Types with zero records are excluded from end events.
  - Type names in logs now match payload record types (e.g. `STEP_COUNT`, `HEART_RATE`).
* **Auto full export on first sync**: `syncNow` now automatically upgrades to full export when the initial sync hasn't been completed, matching iOS behavior.
* **Fixed OkHttp connection leaks**: response bodies are now properly closed in sync payload uploads, token refresh retries, and log requests.

## 0.9.0

* **Smarter token refresh error handling**: token refresh failures are now classified as either `AUTH_FAILURE` (refresh token rejected with 401/403) or `NETWORK_ERROR` (timeout, DNS, 5xx). Only genuine auth failures trigger user disconnect — transient network errors during refresh no longer force sign-out, allowing the SDK's retry mechanism to recover automatically.

## 0.8.0

* **Breaking: Foreground service type changed from `dataSync` to `health`**. Apps must update their Play Console FGS declaration from "Data Sync" to "Health" and remove any manual `<service>` declaration with `foregroundServiceType="dataSync"` from their manifest.
* Replaced `FOREGROUND_SERVICE_DATA_SYNC` permission with `FOREGROUND_SERVICE_HEALTH`.
* Added `HIGH_SAMPLING_RATE_SENSORS` permission to satisfy the `health` FGS runtime prerequisite.
* Updated `HealthSyncWorker.getForegroundInfo()` to pass `FOREGROUND_SERVICE_TYPE_HEALTH`.

## 0.7.0

* **Combined payloads**: all health data types are now merged into a single payload per sync round instead of separate requests per type.
* **Interleaved sync**: data is fetched round-robin across all types (newest to oldest) instead of sequentially type-by-type.
* **Streaming JSON serialization**: replaced in-memory `JsonElement` tree with `android.util.JsonWriter` streaming directly to OkHttp `RequestBody`, fixing `OutOfMemoryError` on large datasets.
* **Bearer prefix normalization**: access tokens returned by the refresh endpoint without the `Bearer ` prefix are now handled correctly.
* **Sign-out reliability**: `EncryptedSharedPreferences.clear()` replaced with individual key removal using `.commit()` to work around a known Android bug where `clear()` may not reliably remove all encrypted entries.
* **`setSyncNotification()`**: customize the foreground notification title and text shown during background sync via WorkManager.
* **Cleaned up logging**: removed verbose Samsung Health SDK reflection logs, per-record debug output, and all token/credential values from log output. Logs now show only essential sync lifecycle events, payload summaries, and HTTP statuses.

## 0.6.0

* Initial tracked release.
