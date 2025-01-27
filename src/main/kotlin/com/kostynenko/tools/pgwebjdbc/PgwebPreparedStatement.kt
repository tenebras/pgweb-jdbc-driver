package com.kostynenko.tools.pgwebjdbc

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.logging.log4j.kotlin.logger
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

class PgwebPreparedStatement(
    connection: PgwebConnection,
    jsonMapper: ObjectMapper,
    private var sql: String
) : PgwebStatement(
    connection,
    jsonMapper
), PreparedStatement {
    
    private val logger = logger()

    init {
        var idx = 1
        sql = sql.replace("\\?".toRegex()) { "$${idx++}" }
    }

    override fun execute(): Boolean = execute(sql)
    override fun executeQuery(): ResultSet = executeQuery(sql)
    override fun executeUpdate(): Int = executeUpdate(sql)

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        logger.todo("PgwebPreparedStatement: setNull")
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        logger.todo("PgwebPreparedStatement: setBoolean")
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        logger.todo("PgwebPreparedStatement: setByte")
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        setValue(parameterIndex, x.toString())
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        setValue(parameterIndex, x.toString())
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        setValue(parameterIndex, x.toString())
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        setValue(parameterIndex, x.toString())
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        setValue(parameterIndex, x.toString())
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        setValue(parameterIndex, x.toString())
    }

    override fun setString(parameterIndex: Int, x: String) {
        sql = sql.replace("$$parameterIndex", "'$x'")
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        logger.todo("PgwebPreparedStatement: setBytes")
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        logger.todo("PgwebPreparedStatement: setDate")
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        logger.todo("PgwebPreparedStatement: setTime")
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        logger.todo("PgwebPreparedStatement: setTimestamp")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.todo("PgwebPreparedStatement: setAsciiStream")
    }

    @Deprecated("Deprecated in Java")
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.todo("PgwebPreparedStatement: setUnicodeStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.todo("PgwebPreparedStatement: setBinaryStream")
    }

    override fun clearParameters() {
        logger.todo("PgwebPreparedStatement: clearParameters")
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        logger.todo("PgwebPreparedStatement: setObject")
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        logger.todo("PgwebPreparedStatement: setObject(parameterIndex: Int, x: Any?)")
    }

    override fun addBatch() {
        logger.todo("PgwebPreparedStatement: addBatch")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) {
        logger.todo("PgwebPreparedStatement: setCharacterStream")
    }

    override fun setRef(parameterIndex: Int, x: Ref?) {
        logger.todo("PgwebPreparedStatement: setRef")
    }

    override fun setBlob(parameterIndex: Int, x: Blob?) {
        logger.todo("PgwebPreparedStatement: setBlob")
    }

    override fun setClob(parameterIndex: Int, x: Clob?) {
        logger.todo("PgwebPreparedStatement: setClob")
    }

    override fun setArray(parameterIndex: Int, x: Array?) {
        logger.todo("PgwebPreparedStatement: setArray")
    }

    override fun getMetaData(): ResultSetMetaData? {
        logger.todo("PgwebPreparedStatement: getmetadata")
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        logger.todo("PgwebPreparedStatement: setDate")
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        logger.todo("PgwebPreparedStatement: setTime")
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        logger.todo("PgwebPreparedStatement: setTimestamp")
    }

    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
        logger.todo("PgwebPreparedStatement: setNull")
    }

    override fun setURL(parameterIndex: Int, x: URL?) {
        logger.todo("PgwebPreparedStatement: setURL")
    }

    override fun getParameterMetaData(): ParameterMetaData? {
        logger.todo("PgwebPreparedStatement: getParameterMetaData")
    }

    override fun setRowId(parameterIndex: Int, x: RowId?) {
        logger.todo("PgwebPreparedStatement: setRowId(parameterIndex: Int, x: RowId?)")
    }

    override fun setNString(parameterIndex: Int, value: String?) {
        logger.todo("PgwebPreparedStatement: setNString(parameterIndex: Int, value: String?)")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) {
        logger.todo("PgwebPreparedStatement: setNCharacterStream")
    }

    override fun setNClob(parameterIndex: Int, value: NClob?) {
        logger.todo("PgwebPreparedStatement: setNClob")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.todo("PgwebPreparedStatement: setClob")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) {
        logger.todo("PgwebPreparedStatement: setBlob")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.todo("PgwebPreparedStatement: setNClob")
    }

    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) {
        logger.todo("PgwebPreparedStatement: setSQLXML")
    }

    override fun setObject(
        parameterIndex: Int,
        x: Any?,
        targetSqlType: Int,
        scaleOrLength: Int
    ) {
        logger.todo("PgwebPreparedStatement: setObject")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.todo("PgwebPreparedStatement: setAsciiStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.todo("PgwebPreparedStatement: setBinaryStream")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.todo("PgwebPreparedStatement: setCharacterStream")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) {
        logger.todo("PgwebPreparedStatement: setAsciiStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) {
        logger.todo("PgwebPreparedStatement: setBinaryStream")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) {
        logger.todo("PgwebPreparedStatement: setCharacterStream")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) {
        logger.todo("PgwebPreparedStatement: setNCharacterStream")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?) {
        logger.todo("PgwebPreparedStatement: setClob")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) {
        logger.todo("PgwebPreparedStatement: setBlob")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?) {
        logger.todo("PgwebPreparedStatement: setNClob")
    }

    private fun setValue(parameterIndex: Int, value: String) {
        sql = sql.replace("$$parameterIndex", value)
    }
}
