package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.logger
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.time.Instant

class PgwebResultSetMetaData(
    private val rawResult: RawPgwebResult
) : ResultSetMetaData {

    private val logger = logger()

    private val columnTypes = mutableMapOf<Int, Triple<Int, String, String>>()

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

    override fun getColumnType(column: Int): Int = resolveColumnType(column).first
    override fun getColumnTypeName(column: Int): String? = resolveColumnType(column).second

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
        logger.info("getColumnClassName($column)")

        return resolveColumnType(column).third
    }

    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (iface.isAssignableFrom(javaClass)) {
            return iface.cast(this)
        }

        throw SQLException("Cannot unwrap to " + iface.getName());
    }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isAssignableFrom(javaClass)

    fun resolveColumnType(column: Int): Triple<Int, String, String> {
        return columnTypes.computeIfAbsent(column) {
            val type = rawResult.rows.map { row ->
                logger.info("resolveColumnType [$column]: $row]")
                val value = row[column - 1]

                when {
                    value == null  -> java.sql.Types.NULL
                    value.toIntOrNull()?.takeIf { it.toString().length == value.length } != null -> java.sql.Types.INTEGER
                    value.toLongOrNull()?.takeIf { it.toString().length == value.length } != null -> java.sql.Types.BIGINT
                    value == "true" || value == "false" -> java.sql.Types.BOOLEAN
                    runCatching { Instant.parse(value) }.getOrNull() != null -> java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                    // timestamp
                    // date
                    else -> java.sql.Types.VARCHAR
                }
            }.filter { it != java.sql.Types.NULL }.distinct().let {
                when {
                    it.size == 1 -> it[0]
                    java.sql.Types.VARCHAR in it -> java.sql.Types.VARCHAR
                    java.sql.Types.BIGINT in it -> java.sql.Types.BIGINT
                    java.sql.Types.INTEGER in it -> java.sql.Types.INTEGER
                    else -> java.sql.Types.VARCHAR
                }
            }

            when (type) {
                java.sql.Types.INTEGER -> Triple(java.sql.Types.INTEGER, "integer", Int::class.java.name)
                java.sql.Types.BIGINT -> Triple(java.sql.Types.BIGINT, "bigint", Long::class.java.name)
                java.sql.Types.BOOLEAN -> Triple(java.sql.Types.BOOLEAN, "bool", Boolean::class.java.name)
                java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> Triple(
                    java.sql.Types.TIMESTAMP_WITH_TIMEZONE,
                    "timestamptz",
                    Instant::class.java.name
                )

                else -> Triple(java.sql.Types.VARCHAR, "text", String::class.java.name)
            }
        }
    }
}