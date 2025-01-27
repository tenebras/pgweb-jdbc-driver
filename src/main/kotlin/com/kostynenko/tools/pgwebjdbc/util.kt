package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.KotlinLogger

fun KotlinLogger.todo(message: String): Nothing {
    debug("Not yet implemented: $message")
    TODO(message)
}