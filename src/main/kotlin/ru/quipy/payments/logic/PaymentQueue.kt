package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val accountName: String,
    val onReject: suspend (UUID, String) -> Unit,
    val onProcess: suspend (PaymentTask) -> Unit
)

class PaymentQueue {
    private val queue = LinkedBlockingQueue<PaymentTask>()
    private val processingTasks = ConcurrentHashMap.newKeySet<UUID>()
    private var isProcessing = false

    companion object {
        val logger = LoggerFactory.getLogger(PaymentQueue::class.java)
    }

    suspend fun addTask(task: PaymentTask) {
        if (now() > task.deadline) {
            task.onReject(task.paymentId, "Expired before queuing")
            return
        }

        queue.put(task)
        logger.info("[${task.accountName}] Payment ${task.paymentId} added to queue, deadline: ${task.deadline}")
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

                    if (!processingTasks.add(task.paymentId)) {
                        logger.warn("Payment ${task.paymentId} is already being processed")
                        continue
                    }

                    launch {
                        try {
                            task.onProcess(task)
                        } finally {
                            processingTasks.remove(task.paymentId)
                        }
                    }
                }
            }
        }
    }

    fun getQueueSize() = queue.size
    fun getProcessingCount() = processingTasks.size
}
