package ru.quipy.payments.logic

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.math.min

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val accountName: String,
    val onReject: suspend (UUID, String) -> Unit,
    val onProcess: suspend (PaymentTask) -> Unit
)

class PaymentQueue(
    private val compositeLimiter: RateLimiter
) {
    private val queue = LinkedBlockingQueue<PaymentTask>()
    private val activeTasks = ConcurrentHashMap.newKeySet<UUID>()
    private var isProcessing = false

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentQueue::class.java)
    }

    suspend fun addTask(task: PaymentTask) {
        if (now() > task.deadline) {
            task.onReject(task.paymentId, "Expired before queuing")
            return
        }
        queue.put(task)
        logger.info("[${task.accountName}] Payment ${task.paymentId} added to queue. QueueSize=${queue.size}, ActiveTasks=${activeTasks.size}")
    }

    fun startProcessing(scope: CoroutineScope, workers: Int = Runtime.getRuntime().availableProcessors()) {
        if (isProcessing) return
        isProcessing = true

        repeat(workers) {
            scope.launch {
                while (true) {
                    val task = queue.take()

                    if (now() > task.deadline) {
                        task.onReject(task.paymentId, "Expired in queue")
                        continue
                    }

                    // Ждем лимитов перед запуском
                    if (!waitForLimitsWithTimeout(task)) {
                        task.onReject(task.paymentId, "Deadline exceeded while waiting for limiter")
                        continue
                    }

                    if (!activeTasks.add(task.paymentId)) {
                        logger.warn("Payment ${task.paymentId} is already active")
                        continue
                    }

                    // Запуск задачи в отдельной корутине, чтобы worker мог брать следующую
                    scope.launch {
                        try {
                            logger.info("[${task.accountName}] Start processing payment=${task.paymentId} on thread=${Thread.currentThread().name}, ActiveTask=${activeTasks.size}")
                            task.onProcess(task)
                        } finally {
                            activeTasks.remove(task.paymentId)
                            logger.info("[${task.accountName}] Finished payment=${task.paymentId}. ActiveWorkers=${activeTasks.size}")
                        }
                    }
                }
            }
        }
    }

    private suspend fun waitForLimitsWithTimeout(task: PaymentTask): Boolean {
        var attempt = 0
        while (now() <= task.deadline) {
            if (compositeLimiter.tick()) return true

            val remaining = task.deadline - now()
            if (remaining <= 0) return false

            val delayTime = calculateExponentialBackoff(attempt++, remaining)
            logger.debug("[${task.accountName}] Waiting for limiter for payment=${task.paymentId}, attempt=$attempt, delay=${delayTime}ms")
            delay(delayTime)
        }
        return false
    }

    private fun calculateExponentialBackoff(attempt: Int, maxWait: Long): Long {
        val baseDelay = 50L
        val defaultMaxDelay = 500L
        val maxShift = 10
        if (maxWait <= 10L) return maxOf(1L, maxWait)
        val effectiveMaxDelay = min(defaultMaxDelay, maxWait)
        val exp = baseDelay * (1L shl min(attempt, maxShift))
        val capped = min(exp, effectiveMaxDelay)
        val jitterSpan = maxOf(1L, capped - baseDelay)
        val jittered = baseDelay + (Math.random() * jitterSpan).toLong()
        val lower = min(10L, maxWait)
        return jittered.coerceIn(lower, maxWait)
    }

    fun getQueueSize() = queue.size
    fun getActiveCount() = activeTasks.size
}
