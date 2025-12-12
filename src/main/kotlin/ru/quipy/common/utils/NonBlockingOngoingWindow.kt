package ru.quipy.common.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    maxWinSize: Int,
) {
    private val window = Semaphore(maxWinSize)

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength

    fun tryAcquire(timeToBlock: Long, timeUnit: TimeUnit): Boolean {
        return window.tryAcquire(timeToBlock, timeUnit)
    }
}

class NonBlockingOngoingWindow(
    private val maxWinSize: Int
) {
    private val winSize = AtomicInteger()

    fun putIntoWindow(): WindowResponse {
        while (true) {
            val currentWinSize = winSize.get()
            if (currentWinSize >= maxWinSize) {
                return WindowResponse.Fail(currentWinSize)
            }

            if (winSize.compareAndSet(currentWinSize, currentWinSize + 1)) {
                break
            }
        }
        return WindowResponse.Success(winSize.get())
    }

    fun releaseWindow() = winSize.decrementAndGet()


    sealed class WindowResponse(val currentWinSize: Int) {
        class Success(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)

        class Fail(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
    }
}