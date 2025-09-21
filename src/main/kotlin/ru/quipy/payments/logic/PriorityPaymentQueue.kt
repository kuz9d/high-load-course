package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val accountName: String,
    val onReject: suspend (UUID, String) -> Unit, // Функция для отклонения
    val onProcess: suspend (PaymentTask) -> Unit   // Функция для обработки
) : Comparable<PaymentTask> {
    override fun compareTo(other: PaymentTask): Int {
        return this.deadline.compareTo(other.deadline)
    }
}

class PriorityPaymentQueue {
    private val queue = PriorityBlockingQueue<PaymentTask>()
    private val processingTasks = mutableSetOf<UUID>()
    private var isProcessing = false

    companion object {
        val logger = LoggerFactory.getLogger(PriorityPaymentQueue::class.java)
    }

    suspend fun addTask(task: PaymentTask) {
        // Немедленная проверка на просроченность
        if (now() > task.deadline) {
            task.onReject(task.paymentId, "Expired before queuing")
            return
        }

        queue.put(task)
        logger.info("[${task.accountName}] Payment ${task.paymentId} added to queue, deadline: ${task.deadline}")
    }

    fun startProcessing(scope: CoroutineScope) {
        if (isProcessing) return
        isProcessing = true

        scope.launch {
            while (true) {
                val task = queue.take()

                // Повторная проверка на случай, если задача просрочилась в очереди
                if (now() > task.deadline) {
                    task.onReject(task.paymentId, "Expired in queue")
                    continue
                }

                // Проверяем, не обрабатывается ли уже этот платеж
                if (task.paymentId in processingTasks) {
                    logger.warn("Payment ${task.paymentId} is already being processed")
                    continue
                }

                processingTasks.add(task.paymentId)
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

    fun getQueueSize() = queue.size
    fun getProcessingCount() = processingTasks.size
}