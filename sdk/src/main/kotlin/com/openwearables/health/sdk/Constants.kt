package com.openwearables.health.sdk

object ProviderIds {
    const val SAMSUNG = "samsung"
    const val GOOGLE = "google"
}

object ProviderDisplayNames {
    const val SAMSUNG_HEALTH = "Samsung Health"
    const val HEALTH_CONNECT = "Health Connect"
}

object SyncDefaults {
    const val SYNC_INTERVAL_MINUTES = 15L
    const val MIN_SYNC_INTERVAL_MINUTES = 15L
    const val CHUNK_SIZE = 2000
    const val WORK_NAME_PERIODIC = "health_sync_periodic"
    const val WORK_NAME_EXPEDITED = "health_sync_expedited"
    const val SDK_VERSION = "0.11.2"

    // Gutsy fork (OW-P3) — retry, serialisation and traversal budgets; see SyncPolicy.kt / SyncRetry.kt.
    const val WORK_NAME_RETRY = "health_sync_retry"
    const val NATIVE_PAGE_BUDGET = 1000
    const val TYPE_READ_FAILURE_LIMIT = 3
    const val SUPERSEDED_RUN_WAIT_MS = 30_000L
    const val RETRY_EARLY_TOLERANCE_MS = 5_000L
    const val SYNC_ATTEMPT_HISTORY = 20
}

object StorageKeys {
    const val SYNC_PREFS_NAME = "com.openwearables.healthsdk.sync"
    const val KEY_ANCHORS = "anchors"
    const val KEY_RETRY_WINDOW = "retryWindow"
    const val KEY_SYNC_ATTEMPTS = "syncAttempts"
    const val SYNC_STATE_DIR = "health_sync_state"
    const val SYNC_STATE_FILE = "state.json"
}

object NotificationConfig {
    const val NOTIFICATION_ID = 9001
    const val CHANNEL_ID = "health_sync_channel"
    const val CHANNEL_NAME = "Health Sync"
    const val CHANNEL_DESCRIPTION = "Background health data synchronization"
    const val DEFAULT_TEXT = "Syncing health data..."
}
