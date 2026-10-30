package com.kostynenko.tools.pgwebjdbc

import tools.jackson.databind.annotation.JsonDeserialize
import java.time.Instant

@JsonDeserialize(using = RawPgwebResultDeserializer::class)
class RawPgwebResult(
    val columns: List<String>,
    val rows: List<List<String?>>,
    val stats: Stats
) {
    fun isUpdateCount() = stats.columnsCount == 1 && columns[0] == "Rows Affected"
    fun isResultSet() = !isUpdateCount()
    fun updateCount(): Int = if (isUpdateCount()) rows[0][0] as Int else -1

    class Stats(
        val columnsCount: Int = 0,
        val rowsCount: Int = 0,
        val rowsAffected: Int = 0,
        val queryStartTime: Instant = Instant.now(),
        val queryFinishTime: Instant = Instant.now(),
        val queryDurationMs: Long = 0
    )
}