package com.kostynenko.tools.pgwebjdbc

import java.time.Instant

data class RawPgwebResult(
    val columns: List<String>,
    val rows: List<List<Any?>>,
    val stats: Stats
) {
    fun isUpdateCount() = stats.columnsCount == 1 && columns[0] == "Rows Affected"
    fun isResultSet() = !isUpdateCount()
    fun updateCount(): Int = if (isUpdateCount()) rows[0][0] as Int else -1

    class Stats(
        val columnsCount: Int,
        val rowsCount: Int,
        val rowsAffected: Int,
        val queryStartTime: Instant,
        val queryFinishTime: Instant,
        val queryDurationMs: Long
    )
}