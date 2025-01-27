package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.logger
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.util.*
import java.util.logging.Logger

class PgwebDriver: Driver {
    private val logger = logger()
    private val connections = mutableMapOf<String, PgwebConnection>()

    init {
        logger.debug("New driver instance")
    }

    companion object {
        @JvmStatic
        val driverInstance = PgwebDriver().also { DriverManager.registerDriver(it)}
    }

    override fun connect(url: String, info: Properties): Connection? {
        if (!acceptsURL(url)) {
            return null
        }

        return connections.computeIfAbsent(url) {
            logger.debug("New connection [$url]")
            val pgwebUrl = url.substringAfter("jdbc:pgweb:").substringBefore(":postgres:")

            PgwebConnection(
                pgwebUrl = pgwebUrl,
                connectionString = url.substringAfter("$pgwebUrl:")
            )
        }
    }

    override fun acceptsURL(url: String): Boolean = url.startsWith("jdbc:pgweb:")

    override fun getPropertyInfo(
        url: String,
        info: Properties
    ): Array<out DriverPropertyInfo> {
        logger.todo("getPropertyInfo")
    }

    override fun getMajorVersion(): Int = 1
    override fun getMinorVersion(): Int = 0
    override fun jdbcCompliant(): Boolean = true
    override fun getParentLogger(): Logger? = null
}
