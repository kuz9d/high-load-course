package ru.quipy.exceptions

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(e: TooManyRequestsException): ResponseEntity<Void> {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build()
    }
}