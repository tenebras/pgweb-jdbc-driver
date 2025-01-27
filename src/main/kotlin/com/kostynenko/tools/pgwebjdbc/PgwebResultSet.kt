package com.kostynenko.tools.pgwebjdbc

import com.kostynenko.tools.pgwebjdbc.RawPgwebResult.Stats
import org.apache.logging.log4j.kotlin.logger
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.time.Instant
import java.util.*

class PgwebResultSet(
    val result: RawPgwebResult
) : ResultSet {
    private var isClosed = false
    private var fetchSize = 1000

    constructor(columns: List<String>, rows: List<List<Any?>>) : this(
        RawPgwebResult(
            columns = columns,
            rows = rows,
            stats = Stats(
                columnsCount = columns.size,
                rowsCount = rows.size,
                rowsAffected = 0,
                queryStartTime = Instant.now(),
                queryFinishTime = Instant.now(),
                queryDurationMs = 0
            )
        )
    )

    private val logger = logger()

    private var rowIdx: Int = -1
    private var isLastValueNull = false
    private fun value(columnIndex: Int): Any? = result.rows[rowIdx][columnIndex - 1].also {
        isLastValueNull = it == null
    }

    override fun next(): Boolean = ++rowIdx < result.stats.rowsCount
    override fun close() {
        isClosed = true
    }

    override fun wasNull(): Boolean = isLastValueNull

    override fun getString(columnIndex: Int): String? = value(columnIndex) as? String
    override fun getBoolean(columnIndex: Int): Boolean =
        getString(columnIndex)?.lowercase() in setOf("true", "yes", "1", "on")

    override fun getByte(columnIndex: Int): Byte {
        logger.todo("PgwebResultSet::getByte")
    }

    override fun getShort(columnIndex: Int): Short {
        val value = value(columnIndex)

        return when (value) {
            is Short -> value
            is Int -> value.toShort()
            is Long -> value.toShort()
            else -> value.toString().toShort()
        }
    }

    override fun getInt(columnIndex: Int): Int {
        val value = value(columnIndex)

        return when (value) {
            is Int -> value
            is Long -> value.toInt()
            is Boolean -> if (value) 1 else 0
            is String -> {
                when (value) {
                    "true" -> 1
                    "false" -> 0
                    else -> value.toInt()
                }
            }

            else -> value.toString().toInt()
        }
    }

    override fun getLong(columnIndex: Int): Long {
        val value = value(columnIndex)

        return when (value) {
            is Long -> value
            is Int -> value.toLong()
            is String -> value.toLong()
            else -> value.toString().toLong()
        }
    }

    override fun getFloat(columnIndex: Int): Float {
        val value = value(columnIndex)

        return when(value) {
            is Float -> value
            is Int -> value.toFloat()
            is Long -> value.toFloat()
            is Double -> value.toFloat()
            else -> value.toString().toFloatOrNull() ?: 0f
        }
    }

    override fun getDouble(columnIndex: Int): Double {
        val value = value(columnIndex)

        return when(value) {
            is Double -> value
            is Float -> value.toDouble()
            is Int -> value.toDouble()
            is Long -> value.toDouble()
            else -> value.toString().toDoubleOrNull() ?: 0.0
        }
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
        logger.todo("PgwebResultSet::getBigDecimal")
    }

    override fun getBytes(columnIndex: Int): ByteArray? {
        logger.todo("PgwebResultSet::getBytes")
    }

    override fun getDate(columnIndex: Int): Date? {
        logger.todo("PgwebResultSet::getDate")
    }

    override fun getTime(columnIndex: Int): Time? {
        logger.todo("PgwebResultSet::getTime")
    }

    override fun getTimestamp(columnIndex: Int): Timestamp? {
        logger.todo("PgwebResultSet::getTimestamp")
    }

    override fun getAsciiStream(columnIndex: Int): InputStream? {
        logger.todo("PgwebResultSet::getAsciiStream")
    }

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnIndex: Int): InputStream? {
        logger.todo("PgwebResultSet::getUnicodeStream")
    }

    override fun getBinaryStream(columnIndex: Int): InputStream? {
        logger.todo("PgwebResultSet::getBinaryStream")
    }

    override fun getString(columnLabel: String): String? = getString(findColumn(columnLabel))
    override fun getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))
    override fun getByte(columnLabel: String): Byte = getByte(findColumn(columnLabel))
    override fun getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))
    override fun getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))
    override fun getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))
    override fun getFloat(columnLabel: String): Float = getFloat(findColumn(columnLabel))
    override fun getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal? {
        logger.todo("PgwebResultSet::getBigDecimal")
    }

    override fun getBytes(columnLabel: String): ByteArray? = getBytes(findColumn(columnLabel))
    override fun getDate(columnLabel: String): Date? = getDate(findColumn(columnLabel))
    override fun getTime(columnLabel: String): Time? = getTime(findColumn(columnLabel))
    override fun getTimestamp(columnLabel: String): Timestamp? = getTimestamp(findColumn(columnLabel))
    override fun getAsciiStream(columnLabel: String): InputStream? = getAsciiStream(findColumn(columnLabel))

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnLabel: String): InputStream? = getUnicodeStream(findColumn(columnLabel))
    override fun getBinaryStream(columnLabel: String): InputStream? = getBinaryStream(findColumn(columnLabel))

    override fun getWarnings(): SQLWarning? {
        logger.todo("PgwebResultSet::getWarnings")
    }

    override fun clearWarnings() {
        logger.todo("PgwebResultSet::clearWarnings")
    }

    override fun getCursorName(): String {
        logger.todo("PgwebResultSet::getCursorName")
    }

    override fun getMetaData(): ResultSetMetaData = PgwebResultSetMetaData(result)

    override fun getObject(columnIndex: Int): Any? {
        return getString(columnIndex)
        //logger.todo("PgwebResultSet::getObject")
    }

    override fun getObject(columnLabel: String): Any? {
        logger.todo("PgwebResultSet::getObject")
    }

    override fun findColumn(columnLabel: String): Int = result.columns.indexOf(columnLabel) + 1

    override fun getCharacterStream(columnIndex: Int): Reader? {
        logger.todo("PgwebResultSet::getCharacterStream")
    }

    override fun getCharacterStream(columnLabel: String): Reader? {
        logger.todo("PgwebResultSet::getCharacterStream")
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        logger.todo("PgwebResultSet::getBigDecimal")
    }

    override fun getBigDecimal(columnLabel: String): BigDecimal? {
        logger.todo("PgwebResultSet::getBigDecimal")
    }

    override fun isBeforeFirst(): Boolean = rowIdx == -1
    override fun isAfterLast(): Boolean = rowIdx >= result.stats.rowsCount
    override fun isFirst(): Boolean = rowIdx == 0
    override fun isLast(): Boolean = rowIdx == result.stats.rowsCount - 1

    override fun beforeFirst() {
        logger.todo("PgwebResultSet::beforeFirst")
    }

    override fun afterLast() {
        logger.todo("PgwebResultSet::afterLast")
    }

    override fun first(): Boolean {
        logger.todo("PgwebResultSet::first")
    }

    override fun last(): Boolean {
        logger.todo("PgwebResultSet::last")
    }

    override fun getRow(): Int = rowIdx + 1

    override fun absolute(row: Int): Boolean {
        logger.todo("PgwebResultSet::absolute")
    }

    override fun relative(rows: Int): Boolean {
        logger.todo("PgwebResultSet::relative")
    }

    override fun previous(): Boolean {
        logger.todo("PgwebResultSet::previous")
    }

    override fun setFetchDirection(direction: Int) {
        logger.todo("PgwebResultSet::setFetchDirection")
    }

    override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD

    override fun setFetchSize(rows: Int) {
        fetchSize = rows
    }

    override fun getFetchSize(): Int = fetchSize
    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

    override fun getConcurrency(): Int {
        logger.todo("PgwebResultSet::getConcurrency")
    }

    override fun rowUpdated(): Boolean {
        logger.todo("PgwebResultSet::rowUpdated")
    }

    override fun rowInserted(): Boolean {
        logger.todo("PgwebResultSet::rowInserted")
    }

    override fun rowDeleted(): Boolean {
        logger.todo("PgwebResultSet::rowDeleted")
    }

    override fun updateNull(columnIndex: Int) {
        logger.todo("PgwebResultSet::updateNull")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        logger.todo("PgwebResultSet::updateBoolean")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        logger.todo("PgwebResultSet::updateByte")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        logger.todo("PgwebResultSet::updateShort")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        logger.todo("PgwebResultSet::updateInt")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        logger.todo("PgwebResultSet::updateLong")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        logger.todo("PgwebResultSet::updateFloat")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        logger.todo("PgwebResultSet::updateDouble")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        logger.todo("PgwebResultSet::updateBigDecimal")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        logger.todo("PgwebResultSet::updateString")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        logger.todo("PgwebResultSet::updateBytes")
    }

    override fun updateDate(columnIndex: Int, x: Date?) {
        logger.todo("PgwebResultSet::updateDate")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        logger.todo("PgwebResultSet::updateTime")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        logger.todo("PgwebResultSet::updateTimestamp")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        logger.todo("PgwebResultSet::updateObject")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        logger.todo("PgwebResultSet::updateObject")
    }

    override fun updateNull(columnLabel: String) {
        logger.todo("PgwebResultSet::updateNull")
    }

    override fun updateBoolean(columnLabel: String, x: Boolean) {
        logger.todo("PgwebResultSet::updateBoolean")
    }

    override fun updateByte(columnLabel: String, x: Byte) {
        logger.todo("PgwebResultSet::updateByte")
    }

    override fun updateShort(columnLabel: String, x: Short) {
        logger.todo("PgwebResultSet::updateShort")
    }

    override fun updateInt(columnLabel: String, x: Int) {
        logger.todo("PgwebResultSet::updateInt")
    }

    override fun updateLong(columnLabel: String, x: Long) {
        logger.todo("PgwebResultSet::updateLong")
    }

    override fun updateFloat(columnLabel: String, x: Float) {
        logger.todo("PgwebResultSet::updateFloat")
    }

    override fun updateDouble(columnLabel: String, x: Double) {
        logger.todo("PgwebResultSet::updateDouble")
    }

    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) {
        logger.todo("PgwebResultSet::updateBigDecimal")
    }

    override fun updateString(columnLabel: String, x: String?) {
        logger.todo("PgwebResultSet::updateString")
    }

    override fun updateBytes(columnLabel: String, x: ByteArray?) {
        logger.todo("PgwebResultSet::updateBytes")
    }

    override fun updateDate(columnLabel: String, x: Date?) {
        logger.todo("PgwebResultSet::updateDate")
    }

    override fun updateTime(columnLabel: String, x: Time?) {
        logger.todo("PgwebResultSet::updateTime")
    }

    override fun updateTimestamp(columnLabel: String, x: Timestamp?) {
        logger.todo("PgwebResultSet::updateTimestamp")
    }

    override fun updateAsciiStream(columnLabel: String, x: InputStream?, length: Int) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Int) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnLabel: String, reader: Reader?, length: Int) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) {
        logger.todo("PgwebResultSet::updateObject")
    }

    override fun updateObject(columnLabel: String, x: Any?) {
        logger.todo("PgwebResultSet::updateObject")
    }

    override fun insertRow() {
        logger.todo("PgwebResultSet::insertRow")
    }

    override fun updateRow() {
        logger.todo("PgwebResultSet::updateRow")
    }

    override fun deleteRow() {
        logger.todo("PgwebResultSet::deleteRow")
    }

    override fun refreshRow() {
        logger.todo("PgwebResultSet::refreshRow")
    }

    override fun cancelRowUpdates() {
        logger.todo("PgwebResultSet::cancelRowUpdates")
    }

    override fun moveToInsertRow() {
        logger.todo("PgwebResultSet::moveToInsertRow")
    }

    override fun moveToCurrentRow() {
        logger.todo("PgwebResultSet::moveToCurrentRow")
    }

    override fun getStatement(): Statement? {
        logger.todo("PgwebResultSet::getStatement")
    }

    override fun getObject(
        columnIndex: Int,
        map: Map<String?, Class<*>?>?
    ): Any? {
        logger.todo("PgwebResultSet::getObject")
    }

    override fun getRef(columnIndex: Int): Ref? {
        logger.todo("PgwebResultSet::getRef")
    }

    override fun getBlob(columnIndex: Int): Blob? {
        logger.todo("PgwebResultSet::getBlob")
    }

    override fun getClob(columnIndex: Int): Clob? {
        logger.todo("PgwebResultSet::getClob")
    }

    override fun getArray(columnIndex: Int): Array? = PgwebArray(this, getString(columnIndex))

    override fun getObject(
        columnLabel: String,
        map: Map<String?, Class<*>?>?
    ): Any? {
        logger.todo("PgwebResultSet::getObject")
    }

    override fun getRef(columnLabel: String): Ref? {
        logger.todo("PgwebResultSet::getRef")
    }

    override fun getBlob(columnLabel: String): Blob? {
        logger.todo("PgwebResultSet::getBlob")
    }

    override fun getClob(columnLabel: String): Clob? {
        logger.todo("PgwebResultSet::getClob")
    }

    override fun getArray(columnLabel: String): Array? = getArray(findColumn(columnLabel))

    override fun getDate(columnIndex: Int, cal: Calendar?): Date? {
        logger.todo("PgwebResultSet::getDate")
    }

    override fun getDate(columnLabel: String, cal: Calendar?): Date? {
        logger.todo("PgwebResultSet::getDate")
    }

    override fun getTime(columnIndex: Int, cal: Calendar?): Time? {
        logger.todo("PgwebResultSet::getTime")
    }

    override fun getTime(columnLabel: String, cal: Calendar?): Time? {
        logger.todo("PgwebResultSet::getTime")
    }

    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? {
        logger.todo("PgwebResultSet::getTimestamp")
    }

    override fun getTimestamp(columnLabel: String, cal: Calendar?): Timestamp? {
        logger.todo("PgwebResultSet::getTimestamp")
    }

    override fun getURL(columnIndex: Int): URL? {
        logger.todo("PgwebResultSet::getURL")
    }

    override fun getURL(columnLabel: String): URL? {
        logger.todo("PgwebResultSet::getURL")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        logger.todo("PgwebResultSet::updateRef")
    }

    override fun updateRef(columnLabel: String, x: Ref?) {
        logger.todo("PgwebResultSet::updateRef")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateBlob(columnLabel: String, x: Blob?) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateClob(columnLabel: String, x: Clob?) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        logger.todo("PgwebResultSet::updateArray")
    }

    override fun updateArray(columnLabel: String, x: Array?) {
        logger.todo("PgwebResultSet::updateArray")
    }

    override fun getRowId(columnIndex: Int): RowId? {
        logger.todo("PgwebResultSet::getRowId")
    }

    override fun getRowId(columnLabel: String): RowId? {
        logger.todo("PgwebResultSet::getRowId")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        logger.todo("PgwebResultSet::updateRowId")
    }

    override fun updateRowId(columnLabel: String, x: RowId?) {
        logger.todo("PgwebResultSet::updateRowId")
    }

    override fun getHoldability(): Int {
        logger.todo("PgwebResultSet::getHoldability")
    }

    override fun isClosed(): Boolean = isClosed

    override fun updateNString(columnIndex: Int, nString: String?) {
        logger.todo("PgwebResultSet::updateNString")
    }

    override fun updateNString(columnLabel: String, nString: String?) {
        logger.todo("PgwebResultSet::updateNString")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun updateNClob(columnLabel: String, nClob: NClob?) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun getNClob(columnIndex: Int): NClob? {
        logger.todo("PgwebResultSet::getNClob")
    }

    override fun getNClob(columnLabel: String): NClob? {
        logger.todo("PgwebResultSet::getNClob")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML? {
        logger.todo("PgwebResultSet::getSQLXML")
    }

    override fun getSQLXML(columnLabel: String): SQLXML? {
        logger.todo("PgwebResultSet::getSQLXML")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        logger.todo("PgwebResultSet::updateSQLXML")
    }

    override fun updateSQLXML(columnLabel: String, xmlObject: SQLXML?) {
        logger.todo("PgwebResultSet::updateSQLXML")
    }

    override fun getNString(columnIndex: Int): String? {
        logger.todo("PgwebResultSet::getNString")
    }

    override fun getNString(columnLabel: String): String? {
        logger.todo("PgwebResultSet::getNString")
    }

    override fun getNCharacterStream(columnIndex: Int): Reader? {
        logger.todo("PgwebResultSet::getNCharacterStream")
    }

    override fun getNCharacterStream(columnLabel: String): Reader? {
        logger.todo("PgwebResultSet::getNCharacterStream")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateNCharacterStream")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateNCharacterStream")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateAsciiStream(columnLabel: String, x: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnLabel: String, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateBlob(columnLabel: String, inputStream: InputStream?, length: Long) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateClob(columnLabel: String, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun updateNClob(columnLabel: String, reader: Reader?, length: Long) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        logger.todo("PgwebResultSet::updateNCharacterStream")
    }

    override fun updateNCharacterStream(columnLabel: String, reader: Reader?) {
        logger.todo("PgwebResultSet::updateNCharacterStream")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateAsciiStream(columnLabel: String, x: InputStream?) {
        logger.todo("PgwebResultSet::updateAsciiStream")
    }

    override fun updateBinaryStream(columnLabel: String, x: InputStream?) {
        logger.todo("PgwebResultSet::updateBinaryStream")
    }

    override fun updateCharacterStream(columnLabel: String, reader: Reader?) {
        logger.todo("PgwebResultSet::updateCharacterStream")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateBlob(columnLabel: String, inputStream: InputStream?) {
        logger.todo("PgwebResultSet::updateBlob")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateClob(columnLabel: String, reader: Reader?) {
        logger.todo("PgwebResultSet::updateClob")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun updateNClob(columnLabel: String, reader: Reader?) {
        logger.todo("PgwebResultSet::updateNClob")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T?>?): T? {
        logger.todo("PgwebResultSet::getObject")
    }

    override fun <T : Any?> getObject(columnLabel: String, type: Class<T?>?): T? {
        logger.todo("PgwebResultSet::getObject")
    }

    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (iface.isAssignableFrom(javaClass)) {
            return iface.cast(this)
        }

        throw SQLException("Cannot unwrap to " + iface.getName());
    }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isAssignableFrom(javaClass)
}