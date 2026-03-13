package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    private val scheduler = Executors.newScheduledThreadPool(100)
    private val semaphore = Semaphore(properties.parallelRequests)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val http2Client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
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

    private val hedgeDelayMs = 165L
    private val hedgeCount = 3

    private val circuitBreakerConfig = CircuitBreakerConfig.custom()
        .failureRateThreshold(50f)
        .waitDurationInOpenState(Duration.ofSeconds(25))
        .permittedNumberOfCallsInHalfOpenState(3)
        .minimumNumberOfCalls(15)
        .slidingWindowSize(100)
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
        .build()

    val circuitBreaker: CircuitBreaker = CircuitBreaker.of("payment-service-$accountName", circuitBreakerConfig)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performPaymentWithHedge(paymentId, amount, transactionId, paymentStartedAt, deadline)
    }

    private fun performPaymentWithHedge(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        fun markPayment(isSuccess: Boolean, reason: String?) {
            paymentESService.update(paymentId) {
                it.logProcessing(success = isSuccess, now(), transactionId, reason = reason)
            }
        }
        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            markPayment(false, "Sliding window exceeded maximum attempts reached")
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = semaphore.tryAcquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            markPayment(false, "Semaphore timeout")
            return
        }

        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .header("x-idempotency-key", transactionId.toString())
            .timeout(Duration.ofMillis(1600))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val start = now()

        val result = CompletableFuture<HttpResponse<String>>()
        val failuresRemaining = AtomicInteger(hedgeCount + 1)

        val decorated = CircuitBreaker.decorateCompletionStage(circuitBreaker) {
            http2Client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        }

        val protectedStage: CompletionStage<HttpResponse<String>> = decorated.get()

        protectedStage.whenCompleteAsync({ r, t ->
            if (result.isDone) return@whenCompleteAsync

            if (t != null) {
                if (t is CallNotPermittedException) {
                    logger.warn("[$accountName] Circuit OPEN -> txId=$transactionId payment=$paymentId skipped")
                } else {
                    logger.debug("[$accountName] error txId=$transactionId", t)
                }

                if (failuresRemaining.decrementAndGet() == 0 && !result.isDone) {
                    result.completeExceptionally(t)
                }
            } else {
                result.complete(r)
            }
        }, scheduler)

        for (i in 1..hedgeCount) {
            scheduler.schedule({
                if (result.isDone) return@schedule
                logger.info("[$accountName] Hedge #$i fired for payment $paymentId, txId: $transactionId")
                retryCounter.increment()

                val hedgeDecorated = CircuitBreaker.decorateCompletionStage(circuitBreaker) {
                    http2Client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                }

                val hedgeProtectedStage: CompletionStage<HttpResponse<String>> = hedgeDecorated.get()

                hedgeProtectedStage.whenCompleteAsync({ r, t ->
                    if (result.isDone) return@whenCompleteAsync

                    if (t != null) {
                        if (t is CallNotPermittedException) {
                            logger.warn("[$accountName] Circuit OPEN -> txId=$transactionId payment=$paymentId hedge #$i skipped")
                        } else {
                            logger.debug("[$accountName] Hedge #$i error txId=$transactionId", t)
                        }

                        if (failuresRemaining.decrementAndGet() == 0 && !result.isDone) {
                            result.completeExceptionally(t)
                        }
                    } else {
                        result.complete(r)
                    }
                }, scheduler)
            }, hedgeDelayMs * i, TimeUnit.MILLISECONDS)
        }

        result.thenApply { response ->
            val elapsed = now() - start
            requestDurationTimer.record(elapsed, TimeUnit.MILLISECONDS)

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            markPayment(body.result, body.message)

            semaphore.release()
        }.exceptionally { ex ->
            when (ex) {
                is CallNotPermittedException -> {
                    logger.error("[$accountName] Circuit breaker OPEN for txId: $transactionId, payment: $paymentId", ex)
                    markPayment(false, "Circuit breaker is open - service unavailable")
                }
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                    markPayment(false, "Request timeout.")
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    markPayment(false, ex.message)
                }
            }
            requestDurationTimer.record(now() - start, TimeUnit.MILLISECONDS)
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()