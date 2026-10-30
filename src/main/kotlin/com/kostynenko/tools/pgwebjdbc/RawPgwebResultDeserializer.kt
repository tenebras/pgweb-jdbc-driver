package com.kostynenko.tools.pgwebjdbc

import tools.jackson.core.JsonParser
import tools.jackson.core.JsonToken
import tools.jackson.databind.DeserializationContext
import tools.jackson.databind.ValueDeserializer

class RawPgwebResultDeserializer: ValueDeserializer<RawPgwebResult?>() {

    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): RawPgwebResult {
        val columns = mutableListOf<String>()
        val rows = mutableListOf<List<String?>>()
        var stats: RawPgwebResult.Stats? = null

        while (p.nextToken() != null) {
            val fieldName = if (p.currentToken() == JsonToken.PROPERTY_NAME) p.currentName() else null

            when (fieldName) {
                "columns" -> {
                    p.nextToken()
                    columns.addAll(readStringList(p))
                }
                "rows" -> {
                    p.nextToken()

                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        rows.add(readNullableStringList(p))
                    }
                }
                "stats" -> {
                    p.nextToken()
                    stats = p.readValueAs(RawPgwebResult.Stats::class.java)
                }
            }
        }

        return RawPgwebResult(columns, rows, stats ?: RawPgwebResult.Stats())
    }

    fun readStringList(p: JsonParser): List<String> = readNullableStringList(p).map { it ?: "" }

    private fun readNullableStringList(p: JsonParser): List<String?> {
        val columns = mutableListOf<String?>()

        while (p.nextToken() != JsonToken.END_ARRAY) {
            if (p.currentToken() == JsonToken.VALUE_NULL) {
                columns.add(null)
            } else {
                columns.add(p.valueAsString)
            }
        }

        return columns
    }
}