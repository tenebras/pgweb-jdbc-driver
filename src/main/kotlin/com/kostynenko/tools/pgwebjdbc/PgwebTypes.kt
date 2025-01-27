package com.kostynenko.tools.pgwebjdbc

import java.sql.Types

// basic pg types info:
// 0 - type name
// 1 - type oid
// 2 - sql type
// 3 - java class
// 4 - array type oid
class Type(
    val name: String,
    val oid: Int,
    val sqlType: Int,
    val javaClass: String,
    val arrayElementType: Type? = null,
    val initialScale: Int = 0,
    val displaySize: Int = 0,
    val precision: Int = 0
) {
    fun scale(mod: Int): Int {
        val actualType = requireNotNull(this.takeIf { arrayElementType == null } ?: arrayElementType) {
            "OID is missing for type [$name]"
        }

        return when (actualType.oid) {
            Oid.NUMERIC -> if (mod == -1) 0 else (mod - 4) and 0xFFFF
            Oid.TIME, Oid.TIMETZ, Oid.TIMESTAMP, Oid.TIMESTAMPTZ -> if (mod == -1) 6 else mod
            Oid.INTERVAL -> if (mod == -1) 6 else mod and 0xFFFF
            else -> initialScale
        }
    }

    fun displaySize(mod: Int): Int {
        val actualType = requireNotNull(this.takeIf { arrayElementType == null } ?: arrayElementType) {
            "OID is missing for type [$name]"
        }
        val unknownLength = Integer.MAX_VALUE

        return when (actualType.oid) {
            Oid.TIME, Oid.TIMETZ, Oid.TIMESTAMP, Oid.TIMESTAMPTZ -> {
                displaySize + when (mod) {
                    -1 -> 7
                    0 -> 0
                    1 -> 3
                    else -> mod + 1
                }
            }
            Oid.VARCHAR, Oid.BPCHAR -> if (mod == -1) unknownLength else mod - 4
            Oid.NUMERIC -> {
                if (mod == -1) {
                    131089
                } else {
                    val precision = (mod - 4 shr 16) and 0xffff;
                    val scale = (mod - 4) and 0xffff;
                    // sign + digits + decimal point (only if we have nonzero scale)
                    return 1 + precision + (if(scale != 0)  1 else 0)
                }
            }
            Oid.BIT -> mod
            Oid.VARBIT -> if (mod == -1) unknownLength else mod
            else -> if (displaySize > 0) displaySize else unknownLength
        }
    }

    fun precision(mod: Int): Int {
        val actualType = requireNotNull(this.takeIf { arrayElementType == null } ?: arrayElementType) {
            "OID is missing for type [$name]"
        }
        val unknownLength = Integer.MAX_VALUE

        return when (actualType.oid) {
            Oid.NUMERIC -> if (mod == -1) 0 else ((mod - 4) and 0xFFFF00) shr 16
            Oid.BPCHAR, Oid.VARCHAR -> if (mod == -1) unknownLength else mod - 4
            Oid.DATE, Oid.TIME, Oid.TIMETZ, Oid.INTERVAL, Oid.TIMESTAMP, Oid.TIMESTAMPTZ ->  displaySize(mod)
            Oid.BIT -> mod
            Oid.VARBIT -> if (mod == -1) unknownLength else mod
            else -> if (precision == 0) unknownLength else precision
        }
    }
}


class PgwebTypes(private val connection: PgwebConnection) { // todo Introduce executor to avoid circular dependencies

    private fun MutableMap<Int, Type>.type(
        pgName: String,
        oid: Int,
        sqlType: Int,
        javaClass: String,
        arrayOid: Int,
        initialScale: Int = 0,
        displaySize: Int = 0,
        precision: Int = 0
    ) {
        val type = Type(
            pgName,
            oid,
            sqlType,
            javaClass,
            initialScale = initialScale,
            displaySize = displaySize,
            precision = precision
        )

        this[oid] = type
        this[arrayOid] = Type(pgName, arrayOid, Types.ARRAY, javaClass, type)
    }

    private val types = mutableMapOf<Int, Type>().apply {
        type("int2", Oid.INT2, Types.INTEGER, "java.lang.Integer", Oid.INT2_ARRAY, displaySize = 6, precision = 5)
        type("int4", Oid.INT4, Types.INTEGER, "java.lang.Integer", Oid.INT4_ARRAY, displaySize = 11, precision = 10)
        type("oid", Oid.OID, Types.BIGINT, "java.lang.Long", Oid.OID_ARRAY, displaySize = 10, precision = 10)
        type("int8", Oid.INT8, Types.BIGINT, "java.lang.Long", Oid.INT8_ARRAY, precision = 19)
        type("money", Oid.MONEY, Types.DOUBLE, "java.lang.Double", Oid.MONEY_ARRAY)
        type("numeric", Oid.NUMERIC, Types.NUMERIC, "java.math.BigDecimal", Oid.NUMERIC_ARRAY)
        type("float4", Oid.FLOAT4, Types.REAL, "java.lang.Float", Oid.FLOAT4_ARRAY, initialScale = 8, displaySize = 15, precision = 8)
        type("float8", Oid.FLOAT8, Types.DOUBLE, "java.lang.Double", Oid.FLOAT8_ARRAY, initialScale = 17, displaySize = 25, precision = 17)
        type("char", Oid.CHAR, Types.CHAR, "java.lang.String", Oid.CHAR_ARRAY, displaySize = 1, precision = 1)
        type("bpchar", Oid.BPCHAR, Types.CHAR, "java.lang.String", Oid.BPCHAR_ARRAY)
        type("varchar", Oid.VARCHAR, Types.VARCHAR, "java.lang.String", Oid.VARCHAR_ARRAY)
        type("varbit", Oid.VARBIT, Types.OTHER, "java.lang.String", Oid.VARBIT_ARRAY)
        type("text", Oid.TEXT, Types.VARCHAR, "java.lang.String", Oid.TEXT_ARRAY)
        type("name", Oid.NAME, Types.VARCHAR, "java.lang.String", Oid.NAME_ARRAY)
        type("bytea", Oid.BYTEA, Types.BINARY, "[B", Oid.BYTEA_ARRAY)
        type("bool", Oid.BOOL, Types.BIT, "java.lang.Boolean", Oid.BOOL_ARRAY, displaySize = 1, precision = 1)
        type("bit", Oid.BIT, Types.BIT, "java.lang.Boolean", Oid.BIT_ARRAY)
        type("date", Oid.DATE, Types.DATE, "java.sql.Date", Oid.DATE_ARRAY, displaySize = 13)
        type("time", Oid.TIME, Types.TIME, "java.sql.Time", Oid.TIME_ARRAY, displaySize = 8)
        type("timetz", Oid.TIMETZ, Types.TIME, "java.sql.Time", Oid.TIMETZ_ARRAY, displaySize = 14)
        type("timestamp", Oid.TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMP_ARRAY, displaySize = 22)
        type("timestamptz", Oid.TIMESTAMPTZ, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMPTZ_ARRAY, displaySize = 28)
        type("refcursor", Oid.REF_CURSOR, Types.REF_CURSOR, "java.sql.ResultSet", Oid.REF_CURSOR_ARRAY)
        type("json", Oid.JSON, Types.OTHER, "java.lang.String", Oid.JSON_ARRAY)
        type("jsonb", Oid.JSONB, Types.OTHER, "java.lang.String", Oid.JSONB_ARRAY)
        type("point", Oid.POINT, Types.OTHER, "org.postgresql.geometric.PGpoint", Oid.POINT_ARRAY)
        type("box", Oid.BOX, Types.OTHER, "org.postgresql.geometric.PGBox", Oid.BOX_ARRAY)
        type("uuid", Oid.UUID, Types.VARCHAR, "java.util.UUID", Oid.UUID_ARRAY)
        type("interval", Oid.INTERVAL, Types.VARCHAR, "java.lang.String", Oid.INTERVAL_ARRAY, displaySize = 49)
    }

    operator fun get(oid: Int): Type = types[oid] ?: findTypeByOid(oid)

    private fun findTypeByOid(oid: Int): Type {
        val sql = """
            SELECT n.nspname = ANY(current_schemas(true)), 
                   n.nspname, 
                   t.typname
             FROM pg_catalog.pg_type t
                  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $oid 
        """

        val resultSet = connection.createStatement().executeQuery(sql).apply {
            next()
        }

        return resultSet.use {
            Type(
                name = it.getString("typname"),
                oid = oid,
                sqlType = Types.OTHER,
                javaClass = "java.lang.String"
            ).also { types[oid] = it }
        }
    }
}
