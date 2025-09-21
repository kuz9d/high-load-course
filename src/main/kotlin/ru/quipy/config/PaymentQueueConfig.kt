package ru.quipy.config

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PriorityPaymentQueue

@Configuration
class PaymentQueueConfig {

    @Bean
    fun priorityPaymentQueue(processingScope: CoroutineScope): PriorityPaymentQueue {
        val queue = PriorityPaymentQueue()
        queue.startProcessing(processingScope)
        return queue
    }

    @Bean
    fun paymentProcessingScope(): CoroutineScope {
        return CoroutineScope(Dispatchers.IO + SupervisorJob())
    }
}