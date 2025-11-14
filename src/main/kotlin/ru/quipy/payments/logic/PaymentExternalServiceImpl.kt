package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.exceptions.isTryRetriableException
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec.toLong()
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        //.callTimeout(1000, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val deadlineViolationCounter = meterRegistry.counter(
        "payments_deadline_violations_total",
        "accountName", accountName
    )

    private val retryCounter = meterRegistry.counter(
        "http_request_retries_total",
        "accountName", accountName
    )

    private val requestDurationTimer: Timer = Timer.builder("payment_request_duration_seconds")
        .description("Duration of external payment requests")
        .publishPercentiles(0.5, 0.8, 0.9, 0.99)
        .publishPercentileHistogram()
        .tag("accountName", accountName)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        fun predictedFinish(): Long = now() + requestAverageProcessingTime.toMillis()
        fun markPayment(isSuccess: Boolean, reason: String) {
            paymentESService.update(paymentId) {
                it.logProcessing(success = isSuccess, now(), transactionId, reason = reason)
            }
        }

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (predictedFinish() > deadline) {
            deadlineViolationCounter.increment()
            markPayment(false, "Deadline")
            return
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        val retryCounterLimit = 4
        var attemptRetry = 0
        var canTry = true

        while (canTry) {
            canTry = false
            attemptRetry++

            try {
                ongoingWindow.acquire()
                rateLimiter.tickBlocking()

                if (predictedFinish() > deadline) {
                    deadlineViolationCounter.increment()
                    markPayment(false, "Deadline")
                    ongoingWindow.release()
                    return
                }

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                val start = System.nanoTime()
                client.newCall(request).execute().use { response ->
                    val elapsed = System.nanoTime() - start
                    requestDurationTimer.record(elapsed, TimeUnit.NANOSECONDS)

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    if (!body.result && body.message == "Temporary error" && attemptRetry < retryCounterLimit) {
                        retryCounter.increment()
                        canTry = true
                        continue
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    markPayment(body.result, body.message ?: "Success")
                }

            } catch (e: java.lang.Exception) {
                when {
                    isTryRetriableException(e) -> {
                        if (attemptRetry < retryCounterLimit && predictedFinish() < deadline) {
                            retryCounter.increment()
                            canTry = true
                            continue
                        }
                        deadlineViolationCounter.increment()
                        markPayment(false, "Deadline")
                        ongoingWindow.release()
                        break
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        markPayment(false, e.message ?: "Unexpected Error")
                        break
                    }
                }
            } finally {
                ongoingWindow.release()
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()