package com.kostynenko.tools.pgwebjdbc

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.logging.log4j.kotlin.logger
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.charset.StandardCharsets
import java.sql.*
import java.sql.Array
import java.util.*
import java.util.concurrent.Executor


class PgwebConnection(
    private val pgwebUrl: String,
    val connectionString: String
) : Connection {
    private val logger = logger()
    private val httpClient = HttpClient.newHttpClient()

    private val connectApiUri by lazy { URI("$pgwebUrl/api/connect") }
    private val queryApiUri by lazy { URI("$pgwebUrl/api/query") }
    private val sessionId = UUID.randomUUID().toString()
    private val jsonMapper = jacksonObjectMapper()
        .findAndRegisterModules()
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    private var connectionResponse: PgwebConnectionResponse? = null
    private val clientInfo = Properties().apply {
        setProperty("ApplicationName", "pgweb-jdbc")
    }

    private var isReadOnly = false
    private var transactionIsolation = Connection.TRANSACTION_NONE

    val types = PgwebTypes(this)

    init {
        logger.info("New connection $sessionId")

        val request = HttpRequest.newBuilder(connectApiUri)
            .headers(
                "Content-Type", "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Session-Id", sessionId
            )
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "url=" + URLEncoder.encode(connectionString, StandardCharsets.UTF_8)
                )
            )
            .build()

        val response = httpClient.send(request, BodyHandlers.ofString())

        if (response.statusCode() != 200) {
            throw SQLException("Failed to connect: ${response.body()}")
        }

        connectionResponse = jsonMapper.readValue<PgwebConnectionResponse>(response.body())
    }

    fun databaseVersion() = requireNotNull(connectionResponse).version
    fun currentUser() = requireNotNull(connectionResponse).currentUser

    override fun createStatement(): Statement = PgwebStatement(this, jsonMapper)
    override fun prepareStatement(sql: String): PreparedStatement = PgwebPreparedStatement(this, jsonMapper, sql)

    override fun prepareCall(sql: String): CallableStatement? {
        logger.todo("prepareCall")
    }

    override fun nativeSQL(sql: String): String {
        logger.info("SQL: $sql")

        val request = HttpRequest.newBuilder(queryApiUri)
            .headers(
                "Content-Type", "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Session-Id", sessionId
            )
            .POST(HttpRequest.BodyPublishers.ofString("query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
            .build()

        val response = httpClient.send(request, BodyHandlers.ofString())
        val body = response.body()

        if (response.statusCode() != 200) {
            println("Request response != 200")
            println(response.statusCode())
            println(body)

            if (response.statusCode() == 400) {
                throw SQLException(jsonMapper.readValue<PgwebError>(body).error)
            }
        }

        return body
    }

    override fun setAutoCommit(autoCommit: Boolean) {}
    override fun getAutoCommit(): Boolean = true

    override fun commit() {
        logger.todo("commit")
    }

    override fun rollback() {
        logger.todo("rollback")
    }

    override fun close() {}
    override fun isClosed(): Boolean = false

    override fun getMetaData(): DatabaseMetaData = PgwebDatabaseMetaData(this)
    override fun setReadOnly(readOnly: Boolean) {
        isReadOnly = readOnly
    }
    override fun isReadOnly(): Boolean = isReadOnly

    override fun setCatalog(catalog: String) {
        logger.info("setCatalog [$catalog]")
    }

    override fun getCatalog(): String = requireNotNull(connectionResponse).currentDatabase

    override fun setTransactionIsolation(level: Int) {
        transactionIsolation = level
    }

    override fun getTransactionIsolation(): Int = transactionIsolation

    override fun getWarnings(): SQLWarning? {
        logger.todo("getWarnings")
    }

    override fun clearWarnings() {
        logger.todo("clearWarnings")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = createStatement()
    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement =
        prepareStatement(sql)

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): CallableStatement? {
        logger.todo("prepareCall")
    }

    override fun getTypeMap(): Map<String?, Class<*>?>? {
        logger.todo("getTypeMap")
    }

    override fun setTypeMap(map: Map<String?, Class<*>?>?) {
        logger.todo("setTypeMap")
    }

    override fun setHoldability(holdability: Int) {
        logger.todo("setHoldability")
    }

    override fun getHoldability(): Int {
        logger.todo("getHoldability")
    }

    override fun setSavepoint(): Savepoint? {
        logger.todo("setSavepoint")
    }

    override fun setSavepoint(name: String?): Savepoint? {
        logger.todo("setSavepoint")
    }

    override fun rollback(savepoint: Savepoint?) {
        logger.todo("rollback")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        logger.todo("releaseSavepoint")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement =
        createStatement()

    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement = prepareStatement(sql)

    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement? {
        logger.todo("prepareCall")
    }

    override fun prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = prepareStatement(sql)
    override fun prepareStatement(sql: String, columnIndexes: IntArray?): PreparedStatement = prepareStatement(sql)
    override fun prepareStatement(sql: String, columnNames: kotlin.Array<out String>?): PreparedStatement =
        prepareStatement(sql)

    override fun createClob(): Clob? {
        logger.todo("createClob")
    }

    override fun createBlob(): Blob? {
        logger.todo("createBlob")
    }

    override fun createNClob(): NClob? {
        logger.todo("createNClob")
    }

    override fun createSQLXML(): SQLXML? {
        logger.todo("createSQLXML")
    }

    override fun isValid(timeout: Int): Boolean = true

    override fun setClientInfo(name: String, value: String) {
        clientInfo.put(name, value)
    }

    override fun setClientInfo(properties: Properties) {
        properties.stringPropertyNames().forEach { name ->
            clientInfo.put(name, properties[name])
        }
    }

    override fun getClientInfo(name: String?): String? = clientInfo[name]?.toString()
    override fun getClientInfo(): Properties = clientInfo

    override fun createArrayOf(typeName: String?, elements: kotlin.Array<out Any?>?): Array? {
        logger.todo("createArrayOf [$typeName, ${elements?.joinToString()}]")
    }

    override fun createStruct(typeName: String?, attributes: kotlin.Array<out Any?>?): Struct? {
        logger.todo("createStruct [$typeName, ${attributes?.joinToString()}]")
    }

    override fun setSchema(schema: String?) {
        logger.todo("setSchema [$schema]")
    }

    override fun getSchema(): String = requireNotNull(connectionResponse).currentSchemas

    override fun abort(executor: Executor?) {
        logger.todo("abort")
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {
        logger.todo("setNetworkTimeout [$milliseconds ms]")
    }

    override fun getNetworkTimeout(): Int {
        logger.todo("getNetworkTimeout")
    }

    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (iface.isAssignableFrom(javaClass)) {
            return iface.cast(this)
        }

        throw SQLException("Cannot unwrap to " + iface.getName());
    }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isAssignableFrom(javaClass)
}
