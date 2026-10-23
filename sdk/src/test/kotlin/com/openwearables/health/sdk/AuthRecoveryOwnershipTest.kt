package com.openwearables.health.sdk

import io.mockk.*
import kotlinx.coroutines.*
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Test
import kotlin.test.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Executes the actual AuthRecovery HTTP callback ladder; only storage/HTTP boundaries are fake. */
class AuthRecoveryOwnershipTest {
    private fun response(code: Int, text: String): Response = Response.Builder()
        .request(Request.Builder().url("https://synthetic.invalid").build())
        .protocol(Protocol.HTTP_1_1).code(code).message("fixture")
        .body(text.toResponseBody("application/json".toMediaType())).build()

    @Test fun lateRefreshSuccessDoesNotInstallInB() = runBlocking {
        exercise(false, 200)
    }
    @Test fun lateRefreshRejectionDoesNotMarkBOrRenewForB() = runBlocking {
        exercise(false, 401)
    }
    @Test fun lateRenewalSuccessDoesNotInstallInB() = runBlocking {
        exercise(true, 200)
    }
    @Test fun lateRenewalRejectionDoesNotClearOrMarkB() = runBlocking {
        exercise(true, 403)
    }
    private suspend fun exercise(renewal: Boolean, delayedStatus: Int) = coroutineScope {
        val storage = mockk<SecureStorage>(relaxed = true)
        val client = mockk<OkHttpClient>()
        var owner = SecureStorage.ConnectionOwner("A", "epoch-A")
        val entered = CountDownLatch(1); val release = CountDownLatch(1)
        val lock = Any()
        every { storage.connectionOwner() } answers { synchronized(lock) { owner } }
        every { storage.isCurrent(any()) } answers { synchronized(lock) { firstArg<SecureStorage.ConnectionOwner>() == owner } }
        every { storage.withConnection<Any?>(any(), any()) } answers {
            synchronized(lock) { if (firstArg<SecureStorage.ConnectionOwner>() == owner) secondArg<() -> Any?>().invoke() else null }
        }
        every { storage.apiBaseUrl } returns "https://synthetic.invalid"
        every { storage.getRefreshToken() } returns "refresh-A"
        every { storage.getRenewalUrl() } returns "https://synthetic.invalid/renew"
        every { storage.getRenewalToken() } returns "renewal-A"
        every { storage.getAuthState() } returns "ok"
        every { storage.getRenewalFailures() } returns 0
        every { storage.getLastRenewalAttemptAt() } returns 0L
        val delayedCall = mockk<okhttp3.Call>()
        every { delayedCall.execute() } answers {
            entered.countDown(); check(release.await(5, TimeUnit.SECONDS))
            response(delayedStatus, "{\"access_token\":\"late-A\",\"refresh_token\":\"late-refresh-A\"}")
        }
        if (renewal) {
            val rejectedCall = mockk<okhttp3.Call>()
            every { rejectedCall.execute() } answers { response(401, "{}") }
            every { client.newCall(any()) } returnsMany listOf(rejectedCall, delayedCall)
        } else every { client.newCall(any()) } returns delayedCall
        val recovery = AuthRecovery(storage, client, {})
        val flight = async(Dispatchers.IO) { recovery.refreshTokens() }
        assertTrue(withContext(Dispatchers.IO) { entered.await(5, TimeUnit.SECONDS) })
        synchronized(lock) { owner = SecureStorage.ConnectionOwner("B", "epoch-B") }
        clearMocks(storage, answers = false, recordedCalls = true, childMocks = false)
        release.countDown()
        assertEquals(AuthRecovery.Result.NETWORK_ERROR, flight.await())
        verify(exactly = 0) { storage.updateTokens(any(), any()) }
        verify(exactly = 0) { storage.saveAuthState(any()) }
        verify(exactly = 0) { storage.setRenewalFailures(any()) }
        verify(exactly = 0) { storage.clearAll() }
        verify(exactly = if (renewal) 2 else 1) { client.newCall(any()) }
    }
}
