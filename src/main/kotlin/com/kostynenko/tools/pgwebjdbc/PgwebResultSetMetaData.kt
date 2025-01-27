package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.logger
import java.sql.ResultSetMetaData
import java.sql.SQLException

class PgwebResultSetMetaData(
    private val rawResult: RawPgwebResult
) : ResultSetMetaData {

    private val logger = logger()

    override fun getColumnCount(): Int = rawResult.stats.columnsCount

    override fun isAutoIncrement(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isAutoIncrement")
    }

    override fun isCaseSensitive(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isCaseSensitive")
    }

    override fun isSearchable(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isSearchable")
    }

    override fun isCurrency(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isCurrency")
    }

    override fun isNullable(column: Int): Int {
        logger.todo("PgwebResultSetMetaData: isNullable")
    }

    override fun isSigned(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isSigned")
    }

    override fun getColumnDisplaySize(column: Int): Int {
        logger.todo("PgwebResultSetMetaData: getColumnDisplaySize")
    }

    override fun getColumnLabel(column: Int): String? {
        require(column > 0 && rawResult.stats.columnsCount >= column)

        return rawResult.columns[column - 1]
    }

    override fun getColumnName(column: Int): String? = getColumnLabel(column)

    override fun getSchemaName(column: Int): String? {
        logger.todo("PgwebResultSetMetaData: getSchemaName")
    }

    override fun getPrecision(column: Int): Int {
        logger.todo("PgwebResultSetMetaData: getPrecision")
    }

    override fun getScale(column: Int): Int {
        logger.todo("PgwebResultSetMetaData: getScale")
    }

    override fun getTableName(column: Int): String? {
        logger.todo("PgwebResultSetMetaData: getTableName")
    }

    override fun getCatalogName(column: Int): String? {
        logger.todo("PgwebResultSetMetaData: getCatalogName")
    }

    override fun getColumnType(column: Int): Int {
        return java.sql.Types.VARCHAR
        //logger.todo("PgwebResultSetMetaData: getColumnType")
    }

    override fun getColumnTypeName(column: Int): String? {
        return "text"
        //logger.todo("PgwebResultSetMetaData: getColumnTypeName")
    }

    override fun isReadOnly(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isReadOnly")
    }

    override fun isWritable(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isWritable")
    }

    override fun isDefinitelyWritable(column: Int): Boolean {
        logger.todo("PgwebResultSetMetaData: isDefinitelyWritable")
    }

    override fun getColumnClassName(column: Int): String? {
        return String::class.java.name
        //logger.todo("PgwebResultSetMetaData: getColumnClassName")
    }

    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (iface.isAssignableFrom(javaClass)) {
            return iface.cast(this)
        }

        throw SQLException("Cannot unwrap to " + iface.getName());
    }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isAssignableFrom(javaClass)
}