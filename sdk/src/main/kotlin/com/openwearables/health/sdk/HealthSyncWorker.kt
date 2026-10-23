package com.openwearables.health.sdk

import android.content.Context
import androidx.work.*

/**
 * WorkManager worker for background health data synchronization.
 *
 * Scheduled as a [PeriodicWorkRequest] by [SyncManager]. The worker does NOT
 * manually schedule the next run — WorkManager handles periodic re-execution.
 */
class HealthSyncWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {

    companion object {
        const val KEY_HOST = "host"
        const val KEY_CUSTOM_SYNC_URL = "customSyncUrl"
        const val KEY_TRIGGER = "trigger"
    }

    override suspend fun doWork(): Result {
        val host = inputData.getString(KEY_HOST) ?: return Result.failure()
        val customSyncUrl = inputData.getString(KEY_CUSTOM_SYNC_URL)
        // Requests enqueued before OW-P3 carry no trigger; only the periodic one outlives an update.
        val trigger = inputData.getString(KEY_TRIGGER) ?: SyncTrigger.PERIODIC

        try {
            setForeground(getForegroundInfo())
        } catch (e: Exception) {
            android.util.Log.w("HealthSyncWorker", "Could not promote to foreground: ${e.message}")
        }

        val secureStorage = SecureStorage(applicationContext)
        val dispatchers = DefaultDispatcherProvider()
        val provider = createProvider(applicationContext, secureStorage, dispatchers)
        val syncManager = SyncManager(
            applicationContext, secureStorage, provider, dispatchers,
            { android.util.Log.d("HealthSyncWorker", it) }
        )

        return try {
            val trackedTypes = secureStorage.getTrackedTypes()
            provider.setTrackedTypes(trackedTypes)

            android.util.Log.d("HealthSyncWorker", "Background sync (provider: ${provider.providerId}, trigger: $trigger)")
            val outcome = syncManager.syncNow(host, customSyncUrl, fullExport = false, trigger = trigger)
            android.util.Log.d("HealthSyncWorker", "Background sync outcome: $outcome")

            // Gutsy fork (OW-P3): a transient failure has already scheduled ONE WorkManager retry at its
            // persisted, jittered backoff, never before Retry-After. Returning retry() as well would
            // stack WorkManager's own un-jittered backoff on top, so every handled outcome succeeds here.
            Result.success()
        } catch (e: Exception) {
            android.util.Log.e("HealthSyncWorker", "Sync failed", e)
            Result.retry()
        }
    }

    private fun createProvider(
        context: Context,
        storage: SecureStorage,
        dispatchers: DispatcherProvider
    ): HealthDataProvider {
        val providerId = storage.getProvider()
        val log: (String) -> Unit = { android.util.Log.d("HealthSyncWorker", it) }
        return when (providerId) {
            ProviderIds.GOOGLE -> HealthConnectManager(context, null, dispatchers, log)
            else -> SamsungHealthManager(context, null, dispatchers, log)
        }
    }

    override suspend fun getForegroundInfo(): ForegroundInfo {
        createNotificationChannel()
        val secureStorage = SecureStorage(applicationContext)
        val notification = androidx.core.app.NotificationCompat.Builder(applicationContext, NotificationConfig.CHANNEL_ID)
            .setContentTitle(secureStorage.getNotificationTitle())
            .setContentText(secureStorage.getNotificationText())
            .setSmallIcon(android.R.drawable.ic_popup_sync)
            .setPriority(androidx.core.app.NotificationCompat.PRIORITY_LOW)
            .setOngoing(true)
            .build()
        return if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.Q) {
            ForegroundInfo(NotificationConfig.NOTIFICATION_ID, notification, android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_HEALTH)
        } else {
            ForegroundInfo(NotificationConfig.NOTIFICATION_ID, notification)
        }
    }

    private fun createNotificationChannel() {
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
            val channel = android.app.NotificationChannel(
                NotificationConfig.CHANNEL_ID,
                NotificationConfig.CHANNEL_NAME,
                android.app.NotificationManager.IMPORTANCE_LOW
            ).apply { description = NotificationConfig.CHANNEL_DESCRIPTION }
            (applicationContext.getSystemService(Context.NOTIFICATION_SERVICE) as android.app.NotificationManager)
                .createNotificationChannel(channel)
        }
    }
}
