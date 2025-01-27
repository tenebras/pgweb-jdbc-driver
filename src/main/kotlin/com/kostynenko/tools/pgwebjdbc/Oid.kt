package com.kostynenko.tools.pgwebjdbc

class Oid {
    companion object {
        const val UNSPECIFIED = 0
        const val INT2 = 21
        const val INT2_ARRAY = 1005
        const val INT4 = 23
        const val INT4_ARRAY = 1007
        const val INT8 = 20
        const val INT8_ARRAY = 1016
        const val TEXT = 25
        const val TEXT_ARRAY = 1009
        const val NUMERIC = 1700
        const val NUMERIC_ARRAY = 1231
        const val FLOAT4 = 700
        const val FLOAT4_ARRAY = 1021
        const val FLOAT8 = 701
        const val FLOAT8_ARRAY = 1022
        const val BOOL = 16
        const val BOOL_ARRAY = 1000
        const val DATE = 1082
        const val DATE_ARRAY = 1182
        const val TIME = 1083
        const val TIME_ARRAY = 1183
        const val TIMETZ = 1266
        const val TIMETZ_ARRAY = 1270
        const val TIMESTAMP = 1114
        const val TIMESTAMP_ARRAY = 1115
        const val TIMESTAMPTZ = 1184
        const val TIMESTAMPTZ_ARRAY = 1185
        const val BYTEA = 17
        const val BYTEA_ARRAY = 1001
        const val VARCHAR = 1043
        const val VARCHAR_ARRAY = 1015
        const val OID = 26
        const val OID_ARRAY = 1028
        const val BPCHAR = 1042
        const val BPCHAR_ARRAY = 1014
        const val MONEY = 790
        const val MONEY_ARRAY = 791
        const val NAME = 19
        const val NAME_ARRAY = 1003
        const val BIT = 1560
        const val BIT_ARRAY = 1561
        const val VOID = 2278
        const val INTERVAL = 1186
        const val INTERVAL_ARRAY = 1187
        const val CHAR = 18 // This is not char(N), this is "char" a single byte type.
        const val CHAR_ARRAY = 1002
        const val VARBIT = 1562
        const val VARBIT_ARRAY = 1563
        const val UUID = 2950
        const val UUID_ARRAY = 2951
        const val XML = 142
        const val XML_ARRAY = 143
        const val POINT = 600
        const val POINT_ARRAY = 1017
        const val BOX = 603
        const val BOX_ARRAY = 1020
        const val JSONB = 3802
        const val JSONB_ARRAY = 3807
        const val JSON = 114
        const val JSON_ARRAY = 199
        const val REF_CURSOR = 1790
        const val REF_CURSOR_ARRAY = 2201
        const val LINE = 628
        const val LSEG = 601
        const val PATH = 602
        const val POLYGON = 604
        const val CIRCLE = 718
        const val CIDR = 650
        const val INET = 869
        const val MACADDR = 829
        const val MACADDR8 = 774
        const val TSVECTOR = 3614
        const val TSQUERY = 3615
    }
}