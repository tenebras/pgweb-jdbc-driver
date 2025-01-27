package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.logger
import java.sql.Array
import java.sql.ResultSet
import java.sql.Types

class PgwebArray(
    private val resultSet: PgwebResultSet,
    rawValue: String?
): Array {
    private val value = rawValue?.let {
        if (rawValue.startsWith('{') && rawValue.endsWith('}')) {
            // values or empty array
            val strings = rawValue.trim('{', '}').split(',').filter { it != "" }
            val longArray = strings.mapNotNull { it.toLongOrNull() }

            longArray.takeIf { it.size == strings.size }?.toTypedArray() ?: strings.toTypedArray()
        } else emptyArray<Long>()
    }

    private val logger = logger()

    override fun getBaseTypeName(): String? = "text"
    override fun getBaseType(): Int = Types.VARCHAR
    override fun getArray(): Any? = value

    override fun getArray(map: Map<String?, Class<*>?>?): Any? {
        logger.todo("PgwebArray:getArray(${map?.entries?.joinToString { "${it.key} = ${it.value?.simpleName}" }})")
    }

    override fun getArray(index: Long, count: Int): Any? {
        logger.todo("PgwebArray:getArray($index, $count)")
    }

    override fun getArray(
        index: Long,
        count: Int,
        map: Map<String?, Class<*>?>?
    ): Any? {
        logger.todo("PgwebArray:getArray($index, $count, ${map?.entries?.joinToString { "${it.key} = ${it.value?.simpleName}" }})")
    }

    override fun getResultSet(): ResultSet = resultSet

    override fun getResultSet(map: Map<String?, Class<*>?>?): ResultSet? {
        logger.todo("getResultSet (${map?.entries?.joinToString { "${it.key} = ${it.value?.simpleName}" }})")
    }

    override fun getResultSet(index: Long, count: Int): ResultSet? {
        logger.todo("getResultSet($index, $count)")
    }

    override fun getResultSet(
        index: Long,
        count: Int,
        map: Map<String?, Class<*>?>?
    ): ResultSet? {
        logger.todo("getResultSet ($index, $count, ${map?.entries?.joinToString { "${it.key} = ${it.value?.simpleName}" }})")
    }

    override fun free() {}
}
