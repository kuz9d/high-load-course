package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val paymentQueue: PaymentQueue,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val parallelLimiter = NonBlockingOngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        requestAverageProcessingTime
    )

    private val compositeLimiter = CompositeRateLimiter(rateLimiter, object : RateLimiter {
        override fun tick(): Boolean {
            return when (parallelLimiter.putIntoWindow()) {
                is NonBlockingOngoingWindow.WindowResponse.Success -> true
                is NonBlockingOngoingWindow.WindowResponse.Fail -> false
            }
        }
    })

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val task = PaymentTask(
            paymentId = paymentId,
            amount = amount,
            paymentStartedAt = paymentStartedAt,
            deadline = deadline,
            accountName = accountName,
            onReject = { id, reason -> rejectPayment(id, reason) },
            onProcess = { task -> processPaymentTask(task) }
        )

        paymentQueue.addTask(task)
    }

    private suspend fun processPaymentTask(task: PaymentTask) {
        if (!waitForLimitsWithTimeout(task.deadline)) {
            rejectPayment(task.paymentId, "Rate limit timeout before deadline")
            return
        }

        val transactionId = UUID.randomUUID()
        try {
            paymentESService.update(task.paymentId) { state ->
                state.logSubmission(
                    success = true,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - task.paymentStartedAt)
                )
            }

            logger.info("[$accountName] Submit: ${task.paymentId} , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=${task.paymentId}&amount=${task.amount}")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val responseBody = response.body?.string()
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${task.paymentId}, result code: ${response.code}, reason: $responseBody")
                    ExternalSysResponse(transactionId.toString(), task.paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${task.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(task.paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }

        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${task.paymentId}", e)
                    paymentESService.update(task.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${task.paymentId}", e)

                    paymentESService.update(task.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            parallelLimiter.releaseWindow()
        }
    }

    private fun rejectPayment(paymentId: UUID, reason: String) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) { state ->
            state.logProcessing(
                success = false,
                processedAt = now(),
                transactionId = transactionId,
                reason = reason
            )
        }
        logger.warn("[$accountName] Payment $paymentId rejected: $reason")
    }

    private suspend fun waitForLimitsWithTimeout(deadline: Long): Boolean {
        var attempt = 0

        while (now() <= deadline) {
            if (compositeLimiter.tick()) {
                return true
            }

            val remainingTime = deadline - now()
            if (remainingTime <= 0) return false

            val backoff = calculateExponentialBackoff(attempt++, remainingTime)
            delay(backoff)
        }

        return false
    }

    private fun calculateExponentialBackoff(attempt: Int, maxWait: Long): Long {
        val baseDelay = 50L
        val defaultMaxDelay = 500L
        val maxShift = 10

        val effectiveMaxDelay = minOf(defaultMaxDelay, maxWait)

        val exponential = baseDelay * (1L shl minOf(attempt, maxShift))
        val capped = minOf(exponential, effectiveMaxDelay)

        val range = (capped - baseDelay).coerceAtLeast(0L)
        val jittered = baseDelay + (Math.random() * range).toLong()

        return jittered.coerceAtMost(maxWait).coerceAtLeast(10L)
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()