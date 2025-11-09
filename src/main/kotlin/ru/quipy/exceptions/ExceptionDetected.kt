package ru.quipy.exceptions

import java.lang.Exception

fun isTryRetriableException(e: Exception): Boolean {
    return e is java.io.InterruptedIOException || e is java.net.SocketException
}