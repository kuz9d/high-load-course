package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import ru.quipy.exceptions.TooManyRequestsException
import java.time.Duration
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class CallerBlockingRejectedExecutionHandler(
    private val maxWait: Duration = Duration.ofMillis(10),
) : RejectedExecutionHandler {
    companion object {
        val logger = LoggerFactory.getLogger(CallerBlockingRejectedExecutionHandler::class.java)
    }

    // Even if event is rejected we will still keep it, trying to put in queue so that not to lose it!
    override fun rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
        if (executor.isShutdown) {
            throw RejectedExecutionException("Executor has been shut down")
        }

        val queue = executor.queue
        try {
            val offered = queue.offer(r, maxWait.toMillis(), TimeUnit.MILLISECONDS)
            if (!offered) {
                logger.warn("Queue full — rejecting task after waiting ${maxWait.seconds}s (queue size: ${queue.size})")
                throw TooManyRequestsException()
            }
        } catch (e: TooManyRequestsException) {
            throw TooManyRequestsException()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RejectedExecutionException("Interrupted while waiting for queue", e)
        }
    }
}