package ru.quipy.exceptions

class TooManyRequestsException(
    message: String = "Too many requests, please retry later"
) : RuntimeException(message)