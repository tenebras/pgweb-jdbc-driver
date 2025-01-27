package com.kostynenko.tools.pgwebjdbc

import org.apache.logging.log4j.kotlin.logger
import java.sql.*
import java.util.*

class PgwebDatabaseMetaData(
    private val connection: PgwebConnection
) : DatabaseMetaData {

    private val logger = logger()

    private var nameDataLength = 0 // length for name datatype
    private var indexMaxKeys = 0 // maximum number of keys in an index.
    private var keywords: String = ""
    private val tableTypeClauses = mapOf<String, Map<String, String>>(
        "TABLE" to mapOf(
            "SCHEMAS" to "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'r' AND c.relname !~ '^pg_'"
        ),
        "PARTITIONED TABLE" to mapOf(
            "SCHEMAS" to "c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'p' AND c.relname !~ '^pg_'"
        ),
        "VIEW" to mapOf(
            "SCHEMAS" to "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'v' AND c.relname !~ '^pg_'"
        ),
        "INDEX" to mapOf(
            "SCHEMAS" to "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'i' AND c.relname !~ '^pg_'"
        ),
        "PARTITIONED INDEX" to mapOf(
            "SCHEMAS" to "c.relkind = 'I' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'I' AND c.relname !~ '^pg_'"
        ),
        "SEQUENCE" to mapOf(
            "SCHEMAS" to "c.relkind = 'S'",
            "NOSCHEMAS" to "c.relkind = 'S'"
        ),
        "TYPE" to mapOf(
            "SCHEMAS" to "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "NOSCHEMAS" to "c.relkind = 'c' AND c.relname !~ '^pg_'"
        ),
        "SYSTEM TABLE" to mapOf(
            "SCHEMAS" to "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')",
            "NOSCHEMAS" to "c.relkind = 'r' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'"
        ),
        "SYSTEM TOAST TABLE" to mapOf(
            "SCHEMAS" to "c.relkind = 'r' AND n.nspname = 'pg_toast'",
            "NOSCHEMAS" to "c.relkind = 'r' AND c.relname ~ '^pg_toast_'"
        ),
        "SYSTEM TOAST INDEX" to mapOf(
            "SCHEMAS" to "c.relkind = 'i' AND n.nspname = 'pg_toast'",
            "NOSCHEMAS" to "c.relkind = 'i' AND c.relname ~ '^pg_toast_'"
        ),
        "SYSTEM VIEW" to mapOf(
            "SCHEMAS" to "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ",
            "NOSCHEMAS" to "c.relkind = 'v' AND c.relname ~ '^pg_'"
        ),
        "SYSTEM INDEX" to mapOf(
            "SCHEMAS" to "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ",
            "NOSCHEMAS" to "c.relkind = 'v' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'"
        ),
        "TEMPORARY TABLE" to mapOf(
            "SCHEMAS" to "c.relkind IN ('r','p') AND n.nspname ~ '^pg_temp_' ",
            "NOSCHEMAS" to "c.relkind IN ('r','p') AND c.relname ~ '^pg_temp_' "
        ),
        "TEMPORARY INDEX" to mapOf(
            "SCHEMAS" to "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ",
            "NOSCHEMAS" to "c.relkind = 'i' AND c.relname ~ '^pg_temp_' "
        ),
        "TEMPORARY VIEW" to mapOf(
            "SCHEMAS" to "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ",
            "NOSCHEMAS" to "c.relkind = 'v' AND c.relname ~ '^pg_temp_' "
        ),
        "TEMPORARY SEQUENCE" to mapOf(
            "SCHEMAS" to "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ",
            "NOSCHEMAS" to "c.relkind = 'S' AND c.relname ~ '^pg_temp_' "
        ),
        "FOREIGN TABLE" to mapOf(
            "SCHEMAS" to "c.relkind = 'f'",
            "NOSCHEMAS" to "c.relkind = 'f'"
        ),
        "MATERIALIZED VIEW" to mapOf(
            "SCHEMAS" to "c.relkind = 'm'",
            "NOSCHEMAS" to "c.relkind = 'm'"
        )
    )

    override fun allProceduresAreCallable(): Boolean = true
    override fun allTablesAreSelectable(): Boolean = true
    override fun getURL(): String = connection.connectionString
    override fun getUserName(): String = connection.currentUser()
    override fun isReadOnly(): Boolean = connection.isReadOnly
    override fun nullsAreSortedHigh(): Boolean = true
    override fun nullsAreSortedLow(): Boolean = false
    override fun nullsAreSortedAtStart(): Boolean = false
    override fun nullsAreSortedAtEnd(): Boolean = false
    override fun getDatabaseProductName(): String = "PostgreSQL"
    override fun getDatabaseProductVersion(): String = connection.databaseVersion()
    override fun getDriverName(): String = "pgweb-jdbc"
    override fun getDriverVersion(): String? = "0.1"
    override fun getDriverMajorVersion(): Int = 0
    override fun getDriverMinorVersion(): Int = 1
    override fun usesLocalFiles(): Boolean = false
    override fun usesLocalFilePerTable(): Boolean = false
    override fun supportsMixedCaseIdentifiers(): Boolean = false
    override fun storesUpperCaseIdentifiers(): Boolean = false
    override fun storesLowerCaseIdentifiers(): Boolean = true
    override fun storesMixedCaseIdentifiers(): Boolean = false
    override fun supportsMixedCaseQuotedIdentifiers(): Boolean = true
    override fun storesUpperCaseQuotedIdentifiers(): Boolean = false
    override fun storesLowerCaseQuotedIdentifiers(): Boolean = false
    override fun storesMixedCaseQuotedIdentifiers(): Boolean = false
    override fun getIdentifierQuoteString(): String = "\""

    override fun getSQLKeywords(): String? {
        if (keywords == "") {
            val sql = """
                select string_agg(word, ',') from pg_catalog.pg_get_keywords() 
                where word <> ALL ('{a,abs,absolute,action,ada,add,admin,after,all,allocate,alter,
                always,and,any,are,array,as,asc,asensitive,assertion,assignment,asymmetric,at,atomic,
                attribute,attributes,authorization,avg,before,begin,bernoulli,between,bigint,binary,
                blob,boolean,both,breadth,by,c,call,called,cardinality,cascade,cascaded,case,cast,
                catalog,catalog_name,ceil,ceiling,chain,char,char_length,character,character_length,
                character_set_catalog,character_set_name,character_set_schema,characteristics,
                characters,check,checked,class_origin,clob,close,coalesce,cobol,code_units,collate,
                collation,collation_catalog,collation_name,collation_schema,collect,column,
                column_name,command_function,command_function_code,commit,committed,condition,
                condition_number,connect,connection_name,constraint,constraint_catalog,constraint_name,
                constraint_schema,constraints,constructors,contains,continue,convert,corr,
                corresponding,count,covar_pop,covar_samp,create,cross,cube,cume_dist,current,
                current_collation,current_date,current_default_transform_group,current_path,
                current_role,current_time,current_timestamp,current_transform_group_for_type,current_user,
                cursor,cursor_name,cycle,data,date,datetime_interval_code,datetime_interval_precision,
                day,deallocate,dec,decimal,declare,default,defaults,deferrable,deferred,defined,definer,
                degree,delete,dense_rank,depth,deref,derived,desc,describe,descriptor,deterministic,
                diagnostics,disconnect,dispatch,distinct,domain,double,drop,dynamic,dynamic_function,
                dynamic_function_code,each,element,else,end,end-exec,equals,escape,every,except,exception,
                exclude,excluding,exec,execute,exists,exp,external,extract,false,fetch,filter,
                final,first,float,floor,following,for,foreign,fortran,found,free,from,full,function,
                fusion,g,general,get,global,go,goto,grant,granted,group,grouping,having,hierarchy,hold,
                hour,identity,immediate,implementation,in,including,increment,indicator,initially,
                inner,inout,input,insensitive,insert,instance,instantiable,int,integer,intersect,
                intersection,interval,into,invoker,is,isolation,join,k,key,key_member,key_type,language,
                large,last,lateral,leading,left,length,level,like,ln,local,localtime,localtimestamp,
                locator,lower,m,map,match,matched,max,maxvalue,member,merge,message_length,
                message_octet_length,message_text,method,min,minute,minvalue,mod,modifies,module,month,
                more,multiset,mumps,name,names,national,natural,nchar,nclob,nesting,new,next,no,none,
                normalize,normalized,not,\"null\",nullable,nullif,nulls,number,numeric,object,
                octet_length,octets,of,old,on,only,open,option,options,or,order,ordering,ordinality,
                others,out,outer,output,over,overlaps,overlay,overriding,pad,parameter,parameter_mode,
                parameter_name,parameter_ordinal_position,parameter_specific_catalog,
                parameter_specific_name,parameter_specific_schema,partial,partition,pascal,path,
                percent_rank,percentile_cont,percentile_disc,placing,pli,position,power,preceding,
                precision,prepare,preserve,primary,prior,privileges,procedure,public,range,rank,read,
                reads,real,recursive,ref,references,referencing,regr_avgx,regr_avgy,regr_count,
                regr_intercept,regr_r2,regr_slope,regr_sxx,regr_sxy,regr_syy,relative,release,
                repeatable,restart,result,return,returned_cardinality,returned_length,
                returned_octet_length,returned_sqlstate,returns,revoke,right,role,rollback,rollup,
                routine,routine_catalog,routine_name,routine_schema,row,row_count,row_number,rows,
                savepoint,scale,schema,schema_name,scope_catalog,scope_name,scope_schema,scroll,
                search,second,section,security,select,self,sensitive,sequence,serializable,server_name,
                session,session_user,set,sets,similar,simple,size,smallint,some,source,space,specific,
                specific_name,specifictype,sql,sqlexception,sqlstate,sqlwarning,sqrt,start,state,
                statement,static,stddev_pop,stddev_samp,structure,style,subclass_origin,submultiset,
                substring,sum,symmetric,system,system_user,table,table_name,tablesample,temporary,then,
                ties,time,timestamp,timezone_hour,timezone_minute,to,top_level_count,trailing,
                transaction,transaction_active,transactions_committed,transactions_rolled_back,
                transform,transforms,translate,translation,treat,trigger,trigger_catalog,trigger_name,
                trigger_schema,trim,true,type,uescape,unbounded,uncommitted,under,union,unique,unknown,
                unnamed,unnest,update,upper,usage,user,user_defined_type_catalog,user_defined_type_code,
                user_defined_type_name,user_defined_type_schema,using,value,values,var_pop,var_samp,
                varchar,varying,view,when,whenever,where,width_bucket,window,with,within,without,work,
                write,year,zone}'::text[])
                """

            val result = createMetaDataStatement().executeQuery(sql).apply {
                next()
            }

            keywords = result.getString(1)
        }

        return keywords
    }

    override fun getNumericFunctions(): String =
        "abs,acos,asin,atan,atan2,ceiling,cos,cot,degrees,exp,floor,log,log10,mod,pi,power,radians,round,sign,sin,sqrt,tan,truncate"

    override fun getStringFunctions(): String =
        "ascii,char,concat,lcase,left,length,ltrim,repeat,rtrim,space,substring,ucase,replace"

    override fun getSystemFunctions(): String = "database,ifnull,user"
    override fun getTimeDateFunctions(): String =
        "curdate,curtime,dayname,dayofmonth,dayofweek,dayofyear,hour,minute,month,monthname,now,quarter,second,week,year,timestampadd"

    override fun getSearchStringEscape(): String = "\\"
    override fun getExtraNameCharacters(): String = ""
    override fun supportsAlterTableWithAddColumn(): Boolean = true
    override fun supportsAlterTableWithDropColumn(): Boolean = true
    override fun supportsColumnAliasing(): Boolean = true
    override fun nullPlusNonNullIsNull(): Boolean = true
    override fun supportsConvert(): Boolean = false
    override fun supportsConvert(fromType: Int, toType: Int): Boolean = false
    override fun supportsTableCorrelationNames(): Boolean = true
    override fun supportsDifferentTableCorrelationNames(): Boolean = false
    override fun supportsExpressionsInOrderBy(): Boolean = true
    override fun supportsOrderByUnrelated(): Boolean = true
    override fun supportsGroupBy(): Boolean = true
    override fun supportsGroupByUnrelated(): Boolean = true
    override fun supportsGroupByBeyondSelect(): Boolean = true
    override fun supportsLikeEscapeClause(): Boolean = true
    override fun supportsMultipleResultSets(): Boolean = true
    override fun supportsMultipleTransactions(): Boolean = true
    override fun supportsNonNullableColumns(): Boolean = true
    override fun supportsMinimumSQLGrammar(): Boolean = true
    override fun supportsCoreSQLGrammar(): Boolean = false
    override fun supportsExtendedSQLGrammar(): Boolean = false
    override fun supportsANSI92EntryLevelSQL(): Boolean = true
    override fun supportsANSI92IntermediateSQL(): Boolean = false
    override fun supportsANSI92FullSQL(): Boolean = false
    override fun supportsIntegrityEnhancementFacility(): Boolean = true
    override fun supportsOuterJoins(): Boolean = true
    override fun supportsFullOuterJoins(): Boolean = true
    override fun supportsLimitedOuterJoins(): Boolean = true
    override fun getSchemaTerm(): String = "schema"
    override fun getProcedureTerm(): String = "function"
    override fun getCatalogTerm(): String = "database"
    override fun isCatalogAtStart(): Boolean = true
    override fun getCatalogSeparator(): String = "."
    override fun supportsSchemasInDataManipulation(): Boolean = true
    override fun supportsSchemasInProcedureCalls(): Boolean = true
    override fun supportsSchemasInTableDefinitions(): Boolean = true
    override fun supportsSchemasInIndexDefinitions(): Boolean = true
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = true
    override fun supportsCatalogsInDataManipulation(): Boolean = false
    override fun supportsCatalogsInProcedureCalls(): Boolean = false
    override fun supportsCatalogsInTableDefinitions(): Boolean = false
    override fun supportsCatalogsInIndexDefinitions(): Boolean = false
    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false
    override fun supportsPositionedDelete(): Boolean = false
    override fun supportsPositionedUpdate(): Boolean = false
    override fun supportsSelectForUpdate(): Boolean = true
    override fun supportsStoredProcedures(): Boolean = true
    override fun supportsSubqueriesInComparisons(): Boolean = true
    override fun supportsSubqueriesInExists(): Boolean = true
    override fun supportsSubqueriesInIns(): Boolean = true
    override fun supportsSubqueriesInQuantifieds(): Boolean = true
    override fun supportsCorrelatedSubqueries(): Boolean = true
    override fun supportsUnion(): Boolean = true
    override fun supportsUnionAll(): Boolean = true
    override fun supportsOpenCursorsAcrossCommit(): Boolean = false
    override fun supportsOpenCursorsAcrossRollback(): Boolean = false
    override fun supportsOpenStatementsAcrossCommit(): Boolean = true
    override fun supportsOpenStatementsAcrossRollback(): Boolean = true
    override fun getMaxBinaryLiteralLength(): Int = 0
    override fun getMaxCharLiteralLength(): Int = 0
    override fun getMaxColumnsInGroupBy(): Int = 0
    override fun getMaxColumnsInOrderBy(): Int = 0
    override fun getMaxColumnsInSelect(): Int = 0
    override fun getMaxColumnNameLength(): Int = getMaxNameLength()
    override fun getMaxColumnsInIndex(): Int = getMaxIndexKeys()
    override fun getMaxColumnsInTable(): Int = 1600
    override fun getMaxConnections(): Int = 8192
    override fun getMaxIndexLength(): Int = 0
    override fun getMaxCursorNameLength(): Int = getMaxNameLength()
    override fun getMaxSchemaNameLength(): Int = getMaxNameLength()
    override fun getMaxProcedureNameLength(): Int = getMaxNameLength()
    override fun getMaxCatalogNameLength(): Int = getMaxNameLength()
    override fun getMaxRowSize(): Int = 1073741824 // 1 GB
    override fun doesMaxRowSizeIncludeBlobs(): Boolean = false
    override fun getMaxStatementLength(): Int = 0
    override fun getMaxStatements(): Int = 0
    override fun getMaxTableNameLength(): Int = getMaxNameLength()
    override fun getMaxTablesInSelect(): Int = 0
    override fun getMaxUserNameLength(): Int = getMaxNameLength()
    override fun supportsTransactions(): Boolean = true

    override fun getDefaultTransactionIsolation(): Int {
        logger.logger.todo("PgwebDatabaseMetaData: getDefaultTransactionIsolation")
    }

    override fun supportsTransactionIsolationLevel(level: Int): Boolean = level in setOf(
        Connection.TRANSACTION_READ_UNCOMMITTED,
        Connection.TRANSACTION_READ_COMMITTED,
        Connection.TRANSACTION_REPEATABLE_READ,
        Connection.TRANSACTION_SERIALIZABLE
    )

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = true
    override fun supportsDataManipulationTransactionsOnly(): Boolean = false
    override fun dataDefinitionCausesTransactionCommit(): Boolean = false
    override fun dataDefinitionIgnoredInTransactions(): Boolean = false

    override fun getProcedures(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?
    ): ResultSet {
        logger.todo("getProcedures($catalog, $schemaPattern, $procedureNamePattern)")

        val sql = """ 
             SELECT NULL AS PROCEDURE_CAT, 
                    n.nspname AS PROCEDURE_SCHEM, 
                    p.proname AS PROCEDURE_NAME, 
                    NULL, 
                    NULL, 
                    NULL, 
                    d.description AS REMARKS, 
                    2 AS PROCEDURE_TYPE, 
                    p.proname || '_' || p.oid AS SPECIFIC_NAME 
               FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc p 
                    LEFT JOIN pg_catalog.pg_description d ON (p.oid=d.objoid) 
                    LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') 
                    LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') 
              WHERE p.pronamespace=n.oid 
                    AND p.prokind='p'
                ${("AND n.nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isNotEmpty() == true } ?: ""}
                ${("AND p.proname LIKE " + escapeQuotes(procedureNamePattern)).takeIf { procedureNamePattern?.isNotEmpty() == true } ?: ""}
           ORDER BY PROCEDURE_SCHEM, 
                    PROCEDURE_NAME, 
                    p.oid::text
        """

        return createMetaDataStatement().executeQuery(sql)
    }

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet? {
        logger.logger.todo("PgwebDatabaseMetaData: getProcedureColumns")
    }

    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        logger.debug("getTables ($catalog, $schemaPattern, $tableNamePattern, ${types?.joinToString(",")})")
        val sql = """
            SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, 
         CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' 
         WHEN true THEN CASE 
         WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind 
          WHEN 'r' THEN 'SYSTEM TABLE' 
          WHEN 'v' THEN 'SYSTEM VIEW' 
          WHEN 'i' THEN 'SYSTEM INDEX' 
          ELSE NULL 
          END 
         WHEN n.nspname = 'pg_toast' THEN CASE c.relkind 
          WHEN 'r' THEN 'SYSTEM TOAST TABLE' 
          WHEN 'i' THEN 'SYSTEM TOAST INDEX' 
          ELSE NULL 
          END 
         ELSE CASE c.relkind 
          WHEN 'r' THEN 'TEMPORARY TABLE' 
          WHEN 'p' THEN 'TEMPORARY TABLE' 
          WHEN 'i' THEN 'TEMPORARY INDEX' 
          WHEN 'S' THEN 'TEMPORARY SEQUENCE' 
          WHEN 'v' THEN 'TEMPORARY VIEW' 
          ELSE NULL 
          END 
         END 
         WHEN false THEN CASE c.relkind 
         WHEN 'r' THEN 'TABLE' 
         WHEN 'p' THEN 'PARTITIONED TABLE' 
         WHEN 'i' THEN 'INDEX' 
         WHEN 'P' then 'PARTITIONED INDEX' 
         WHEN 'S' THEN 'SEQUENCE' 
         WHEN 'v' THEN 'VIEW' 
         WHEN 'c' THEN 'TYPE' 
         WHEN 'f' THEN 'FOREIGN TABLE' 
         WHEN 'm' THEN 'MATERIALIZED VIEW' 
         ELSE NULL 
         END 
         ELSE NULL 
         END 
         AS TABLE_TYPE, d.description AS REMARKS, 
         '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, 
        '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION 
         FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c 
         LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass) 
         WHERE c.relnamespace = n.oid
          ${(" AND n.nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isEmpty() == false } ?: ""}
          ${(" AND c.relname LIKE " + escapeQuotes(tableNamePattern)).takeIf { tableNamePattern?.isEmpty() == false } ?: ""}
          ${
            types?.mapNotNull { tableTypeClauses[it]?.get("SCHEMAS")?.let { "($it)" } }?.joinToString(" OR ")?.let {
                " AND (false OR $it)"
            } ?: ""
        }
        ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME
        """

        return (createMetaDataStatement().executeQuery(sql) as PgwebResultSet)
            .withUpperCaseColumnLabels()
    }

    override fun getSchemas(): ResultSet = getSchemas(null, null)

    override fun getCatalogs(): ResultSet = createMetaDataStatement().executeQuery(
        """
        SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database
            WHERE datallowconn = true
            ORDER BY datname
    """
    )

    override fun getTableTypes(): ResultSet = PgwebResultSet(
        columns = listOf("TABLE_TYPE"),
        rows = tableTypeClauses.keys.sorted().map { listOf(it) }
    )

    override fun getColumns(
        catalog: String,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        logger.debug("getColumns of [$catalog, $schemaPattern, $tableNamePattern, $columnNamePattern]")

        val sql = """
            SELECT * FROM (
                SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull 
                OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,
                row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, 
                nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,
                pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype 
                FROM pg_catalog.pg_namespace n 
                JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) 
                JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) 
                JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) 
                LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) 
                LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) 
                LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') 
                LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') 
                WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped
                ${(" AND n.nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isNotEmpty() == true } ?: ""}
                ${(" AND c.relname LIKE " + escapeQuotes(tableNamePattern)).takeIf { tableNamePattern?.isNotEmpty() == true } ?: ""}
                ) c WHERE true
                ${(" AND attname LIKE " + escapeQuotes(columnNamePattern)).takeIf { columnNamePattern?.isNotEmpty() == true } ?: ""}
                ORDER BY nspname,c.relname,attnum 
        """
        val rs = connection.createStatement().executeQuery(sql)
        val rows = mutableListOf<List<Any?>>()

        while (rs.next()) {
            val typeOid = rs.getInt("atttypid")
            val typeMod = rs.getInt("atttypmod")
            val type = connection.types[typeOid]

            val sqlType = when (rs.getString("typtype")) {
                "c" -> Types.STRUCT
                "d" -> Types.DISTINCT
                "e" -> Types.VARCHAR
                else -> type.sqlType
            }

            val baseTypeOid = rs.getInt("typbasetype")
            var decimalDigits = 0
            var columnSize = 0

            /* this is really a DOMAIN type not sure where DISTINCT came from */
            if (sqlType == Types.DISTINCT) {
                val baseType = connection.types[baseTypeOid]
                var typtypmod = rs.getInt("typtypmod")

                decimalDigits = baseType.scale(typeMod)

                if (typtypmod == -1) {
                    columnSize = baseType.precision(typeMod)
                } else if (baseTypeOid == Oid.NUMERIC) {
                    decimalDigits = baseType.scale(typtypmod)
                    columnSize = baseType.precision(typtypmod)
                } else {
                    columnSize = typtypmod
                }
            } else {
                decimalDigits = type.scale(typeMod)
                columnSize = type.precision(typeMod)

                if (sqlType != Types.NUMERIC && columnSize == 0) {
                    columnSize = type.displaySize(typeMod)
                }
            }

            rows.add(
                listOf(
                    null, // Catalog name, not supported
                    rs.getString("nspname"), // Schema
                    rs.getString("relname"), // Table name
                    rs.getString("attname"), // Column name
                    sqlType,
                    // Type name
                    if (rs.getString("adsrc")?.contains("nextval(") == true) {
                        when (type.name) {
                            "int4" -> "serial"
                            "int8" -> "bigserial"
                            "int2" -> "smallserial"
                            else -> type.name
                        }
                    } else {
                        type.name
                    },
                    columnSize,//6
                    null, // Buffer length
                    if ((sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) && typeMod == -1) {
                        null
                    } else {
                        decimalDigits.toString()
                    },
                    "2".takeIf { "bit" == type.name || "varbit" == type.name } ?: "10",
                    if (rs.getBoolean("attnotnull")) 0 else 1,
                    rs.getString("description"), // Description (if any)
                    rs.getString("adsrc"), // Column default
                    null, // sql data type (unused)
                    null, // sql datetime sub (unused)
                    columnSize, // char octet length
                    rs.getString("attnum"), // ordinal position
                    if (rs.getBoolean("attnotnull")) "NO" else "YES", // Is nullable
                    null, // SCOPE_CATLOG
                    null, // SCOPE_SCHEMA
                    null, // SCOPE_TABLE
                    // SOURCE_DATA_TYPE
                    if (baseTypeOid == 0) null else connection.types[baseTypeOid].sqlType
                )
            )

        }

        return PgwebResultSet(
            columns = listOf(
                "TABLE_CAT", //0
                "TABLE_SCHEM", //1
                "TABLE_NAME", //2
                "COLUMN_NAME", //3
                "DATA_TYPE", //4
                "TYPE_NAME", //5
                "COLUMN_SIZE", //6
                "BUFFER_LENGTH", //7
                "DECIMAL_DIGITS", //8
                "NUM_PREC_RADIX", //9
                "NULLABLE", //10
                "REMARKS", //11
                "COLUMN_DEF", //12
                "SQL_DATA_TYPE", //13
                "SQL_DATETIME_SUB", //14
                "CHAR_OCTET_LENGTH", //15
                "ORDINAL_POSITION", //16
                "IS_NULLABLE", //17
                "SCOPE_CATALOG", //18
                "SCOPE_SCHEMA", //19
                "SCOPE_TABLE", //20
                "SOURCE_DATA_TYPE",//21
                "IS_AUTOINCREMENT", //22
                "IS_GENERATEDCOLUMN"//23
            ),
            rows = rows
        )
    }

    override fun getColumnPrivileges(
        catalog: String?,
        schema: String?,
        table: String?,
        columnNamePattern: String?
    ): ResultSet? {
        logger.logger.todo("PgwebDatabaseMetaData: getColumnPrivileges")
    }

    override fun getTablePrivileges(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?
    ): ResultSet? {
        logger.logger.todo("PgwebDatabaseMetaData: getTablePrivileges")
    }

    override fun getBestRowIdentifier(
        catalog: String?,
        schema: String?,
        table: String?,
        scope: Int,
        nullable: Boolean
    ): ResultSet? {
        logger.logger.todo("PgwebDatabaseMetaData: getBestRowIdentifier")
    }

    override fun getVersionColumns(
        catalog: String?,
        schema: String?,
        table: String?
    ): ResultSet? {
        logger.logger.todo("PgwebDatabaseMetaData: getVersionColumns")
    }

    override fun getPrimaryKeys(
        catalog: String?,
        schema: String?,
        table: String?
    ): ResultSet {
        val sql = """
            SELECT NULL AS TABLE_CAT, 
                   n.nspname AS TABLE_SCHEM, 
                   ct.relname AS TABLE_NAME, 
                   a.attname AS COLUMN_NAME, 
                   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, 
                   ci.relname AS PK_NAME, 
                   information_schema._pg_expandarray(i.indkey) AS KEYS, 
                   a.attnum AS A_ATTNUM 
              FROM pg_catalog.pg_class ct 
                   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) 
                   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) 
                   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) 
                   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) 
             WHERE true
             ${(" AND n.nspname = " + escapeQuotes(schema)).takeIf { schema?.isNotEmpty() == true }}
             ${(" AND ct.relname = " + escapeQuotes(table)).takeIf { table?.isNotEmpty() == true }}
             AND i.indisprimary 
        """

        return createMetaDataStatement().executeQuery(
            """
            SELECT result.TABLE_CAT, 
                   result.TABLE_SCHEM, 
                   result.TABLE_NAME, 
                   result.COLUMN_NAME, 
                   result.KEY_SEQ, 
                   result.PK_NAME 
              FROM (${sql}) result
             WHERE result.A_ATTNUM = (result.KEYS).x
             ORDER BY result.table_name, result.pk_name, result.key_seq
            """
        )
    }

    override fun getImportedKeys(
        catalog: String?,
        schema: String?,
        table: String?
    ): ResultSet = getImportedExportedKeys(null, null, null, catalog, schema, table)

    override fun getExportedKeys(
        catalog: String?,
        schema: String?,
        table: String?
    ): ResultSet = getImportedExportedKeys(catalog, schema, table, null, null, null)

    override fun getCrossReference(
        primaryCatalog: String?,
        primarySchema: String?,
        primaryTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet = getImportedExportedKeys(primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable)

    override fun getTypeInfo(): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: getTypeInfo")
    }

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean
    ): ResultSet? {
        logger.debug("getIndexInfo [$catalog, $schema, $table, $unique, $approximate]")

        val subquery = """
            SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, 
              ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, 
              NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, 
              CASE i.indisclustered 
                WHEN true THEN 1
                ELSE CASE am.amname 
                  WHEN 'hash' THEN 2
                  ELSE 3
                END 
              END AS TYPE, 
              (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, 
              ci.reltuples AS CARDINALITY, 
              ci.relpages AS PAGES, 
              pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION, 
              ci.oid AS CI_OID, 
              i.indoption AS I_INDOPTION, 
              am.amname AS AM_NAME
            FROM pg_catalog.pg_class ct 
              JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) 
              JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) 
              JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) 
              JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) 
            WHERE true 
            ${(" AND n.nspname = " + escapeQuotes(schema)).takeIf { schema?.isNotEmpty() == true } ?: ""}
             AND ct.relname = ${escapeQuotes(table)}
            ${" AND i.indisunique ".takeIf { unique } ?: ""}
            """

            val sql = """
                SELECT 
                tmp.TABLE_CAT, 
                tmp.TABLE_SCHEM, 
                tmp.TABLE_NAME, 
                tmp.NON_UNIQUE, 
                tmp.INDEX_QUALIFIER, 
                tmp.INDEX_NAME, 
                tmp.TYPE, 
                tmp.ORDINAL_POSITION, 
                trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS COLUMN_NAME, 
              CASE tmp.AM_NAME 
                WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint 
                  WHEN 1 THEN 'D' 
                  ELSE 'A' 
                END 
                ELSE NULL 
              END AS ASC_OR_DESC, 
                tmp.CARDINALITY, 
                tmp.PAGES, 
                tmp.FILTER_CONDITION 
            FROM ( $subquery ) AS tmp
            ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION
            """

        return (createMetaDataStatement().executeQuery(sql) as PgwebResultSet)
            .withUpperCaseColumnLabels()
    }

    override fun supportsResultSetType(type: Int): Boolean = type != ResultSet.TYPE_SCROLL_SENSITIVE

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean = when (type) {
        ResultSet.TYPE_SCROLL_SENSITIVE -> false
        else -> true
    }

    override fun ownUpdatesAreVisible(type: Int): Boolean = true
    override fun ownDeletesAreVisible(type: Int): Boolean = true
    override fun ownInsertsAreVisible(type: Int): Boolean = true
    override fun othersUpdatesAreVisible(type: Int): Boolean = false
    override fun othersDeletesAreVisible(type: Int): Boolean = false
    override fun othersInsertsAreVisible(type: Int): Boolean = false
    override fun updatesAreDetected(type: Int): Boolean = false
    override fun deletesAreDetected(type: Int): Boolean = false
    override fun insertsAreDetected(type: Int): Boolean = false
    override fun supportsBatchUpdates(): Boolean = true

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: getUDTs")
    }

    override fun getConnection(): Connection = connection
    override fun supportsSavepoints(): Boolean = true
    override fun supportsNamedParameters(): Boolean = false
    override fun supportsMultipleOpenResults(): Boolean = false
    override fun supportsGetGeneratedKeys(): Boolean = true

    override fun getSuperTypes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?
    ): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: Not yet implemented: getSuperTypes(String,String,String)")
    }

    override fun getSuperTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?
    ): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: Not yet implemented: getSuperTables(String,String,String,String)")
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: Not yet implemented: getAttributes(String,String,String,String)")
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean = true
    override fun getResultSetHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun getDatabaseMajorVersion(): Int =
        connection.databaseVersion().substringAfter("PostgreSQL ").substringBefore('.').toInt()

    override fun getDatabaseMinorVersion(): Int =
        connection.databaseVersion().substringAfter('.').substringBefore(' ').toInt()

    override fun getJDBCMajorVersion(): Int = 4
    override fun getJDBCMinorVersion(): Int = 2

    override fun getSQLStateType(): Int = 2
    override fun locatorsUpdateCopy(): Boolean = true
    override fun supportsStatementPooling(): Boolean = false

    override fun getRowIdLifetime(): RowIdLifetime {
        logger.todo("PgwebDatabaseMetaData: getRowIdLifetime")
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        var sql = """ 
             SELECT nspname AS TABLE_SCHEM, 
                    NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace 
              WHERE nspname <> 'pg_toast' 
                    AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) 
                    AND (nspname !~ '^pg_toast_temp_' OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
                ${("AND nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isNotEmpty() == true } ?: ""}
           ORDER BY TABLE_SCHEM
            """
//        if (connection.getHideUnprivilegedObjects()) {
//            sql += " AND has_schema_privilege(nspname, 'USAGE, CREATE')";
//        }

        return createMetaDataStatement().executeQuery(sql)
    }

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = true
    override fun autoCommitFailureClosesAllResultSets(): Boolean = false

    override fun getClientInfoProperties(): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: getClientInfoProperties")
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        val sql = """ 
             SELECT current_database() AS FUNCTION_CAT, 
                    n.nspname AS FUNCTION_SCHEM, 
                    p.proname AS FUNCTION_NAME, 
                    d.description AS REMARKS, 
                    CASE
                        WHEN (format_type(p.prorettype, null) = 'unknown') THEN 0
                        WHEN (substring(pg_get_function_result(p.oid) from 0 for 6) = 'TABLE') OR (substring(pg_get_function_result(p.oid) from 0 for 6) = 'SETOF') THEN 2
                        ELSE 1
                    END AS FUNCTION_TYPE, 
                    p.proname || '_' || p.oid AS SPECIFIC_NAME 
               FROM pg_catalog.pg_proc p 
                    INNER JOIN pg_catalog.pg_namespace n ON p.pronamespace=n.oid 
                    LEFT JOIN pg_catalog.pg_description d ON p.oid=d.objoid 
              WHERE true  
                    AND p.prokind='f'
                ${("AND n.nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isNotEmpty() == true } ?: ""}
                ${("AND p.proname LIKE " + escapeQuotes(functionNamePattern)).takeIf { functionNamePattern?.isNotEmpty() == true } ?: ""}
              ORDER BY FUNCTION_SCHEM, FUNCTION_NAME, p.oid::text
        """

        return createMetaDataStatement().executeQuery(sql)
    }

    private fun ResultSet.getArrayAsListOrNull(name: String) = getString(name)?.trim('{', '}')?.split(',')

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet? {
        val columns = listOf(
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "COLUMN_NAME",
            "COLUMN_TYPE",
            "DATA_TYPE",
            "TYPE_NAME",
            "PRECISION",
            "LENGTH",
            "SCALE",
            "RADIX",
            "NULLABLE",
            "REMARKS",
            "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION",
            "IS_NULLABLE",
            "SPECIFIC_NAME",
        );// columns

        val sql = """ 
             SELECT n.nspname,
                    p.proname,p.prorettype,
                    p.proargtypes, 
                    t.typtype,
                    t.typrelid, 
                    p.proargnames, 
                    p.proargmodes, 
                    p.proallargtypes, 
                    p.oid 
               FROM pg_catalog.pg_proc p, 
                    pg_catalog.pg_namespace n, 
                    pg_catalog.pg_type t 
              WHERE p.pronamespace=n.oid 
                    AND p.prorettype=t.oid
                ${("AND n.nspname LIKE " + escapeQuotes(schemaPattern)).takeIf { schemaPattern?.isNotEmpty() == true } ?: ""}
                ${("AND p.proname LIKE " + escapeQuotes(functionNamePattern)).takeIf { functionNamePattern?.isNotEmpty() == true } ?: ""}
           ORDER BY n.nspname, 
                    p.proname, 
                    p.oid::text
        """
        val rows = mutableListOf<List<Any?>>();// rows
//        byte[] isnullableUnknown = new byte[0];

        val stmt = connection.createStatement()
        val rs = stmt.executeQuery(sql)

        while (rs.next()) {
            val schema = rs.getString("nspname")
            val functionName = rs.getString("proname")
            val specificName = rs.getString("proname") + "_" + rs.getString("oid")
            val returnType = rs.getInt("prorettype")
            val returnTypeType = rs.getString("typtype")
            val returnTypeRelid = rs.getLong("typrelid")

            val strArgTypes = rs.getString("proargtypes")
            val st = StringTokenizer(strArgTypes)
            val argTypes = mutableListOf<Long>()

            while (st.hasMoreTokens()) {
                argTypes.add(st.nextToken().toLong())
            }

            val argNames = rs.getString("proargnames")?.trim('{', '}')?.split(',') ?: emptyList()
            val argModes = rs.getArrayAsListOrNull("proargmodes")
            val numArgs = argTypes.size
            val allArgTypes = rs.getArrayAsListOrNull("proallargtypes")

            // decide if we are returning a single column result.
            if ("b" == returnTypeType || "d" == returnTypeType || "e" == returnTypeType || ("p" == returnTypeType && argModes == null)) {
                val type = connection.types[returnType]
                rows.add(
                    listOf(
                        null,
                        schema,
                        functionName,
                        "returnValue",
                        "4",
                        type.sqlType,
                        type.name,
                        null,
                        null,
                        null,
                        null,
                        "2",
                        null,
                        "0",
                        "",//isnullableUnknown,
                        specificName
                    )
                )
            }

            // Add a row for each argument.
            (0 until numArgs).forEach { i ->
                val argOid = (allArgTypes?.get(i)?.toInt() ?: argTypes[i].toInt())
                val type = connection.types[argOid]
                rows.add(
                    listOf(
                        null,
                        schema,
                        functionName,
                        if (argNames.isNotEmpty()) argNames[i] else "$" + (i + 1),
                        when (argModes?.get(i)) {
                            "0" -> 3
                            "b" -> 2
                            "t" -> 4
                            else -> 1
                        }.toString(),
                        type.sqlType.toString(),
                        type.name,
                        null,
                        null,
                        null,
                        null,
                        "2",
                        null,
                        (i + 1).toString(),
                        "",
                        specificName
                    )
                )
            }

            // if we are returning a multi-column result.
            if ("c" == returnTypeType || ("p" == returnTypeType && argModes != null)) {
                val columnsql = """ 
                    SELECT a.attname,
                           a.atttypid 
                      FROM pg_catalog.pg_attribute a 
                     WHERE a.attrelid = $returnTypeRelid 
                           AND NOT a.attisdropped 
                           AND a.attnum > 0 ORDER BY a.attnum 
                """

                val columnrs = connection.createStatement().executeQuery(columnsql)

                while (columnrs.next()) {
                    val columnTypeOid = columnrs.getInt("atttypid")
                    val type = connection.types[columnTypeOid]
                    val tuple = listOf(
                        null,
                        schema,
                        functionName,
                        columnrs.getString("attname"),
                        "5",
                        type.sqlType,
                        type.name,
                        null,
                        null,
                        null,
                        null,
                        "2",
                        null,
                        "0",
                        "",// isnullableUnknown;
                        specificName
                    )

                    rows.add(tuple)
                }
            }
        }

        return PgwebResultSet(
            columns = columns,
            rows = rows
        )
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet? {
        logger.todo("PgwebDatabaseMetaData: getPseudoColumns(String, String, String, String)")
    }

    override fun generatedKeyAlwaysReturned(): Boolean = true

    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (iface.isAssignableFrom(javaClass)) {
            return iface.cast(this)
        }

        throw SQLException("Cannot unwrap to " + iface.getName())
    }

    override fun isWrapperFor(iface: Class<*>): Boolean = iface.isAssignableFrom(javaClass)

    private fun escapeQuotes(s: String?): String {
        val sb = StringBuilder()

//        if (!connection.getStandardConformingStrings()) {
//            sb.append("E")
//        }

        sb.append("'")
        sb.append(s) //sb.append(connection.escapeString(s))
        sb.append("'")

        return sb.toString()
    }

    private fun getMaxNameLength(): Int {
        if (nameDataLength == 0) {
            val sql = "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n " +
                "WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'"

            connection.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    if (!rs.next()) {
                        throw SQLException("Unable to find name datatype in the system catalogs.")
                    }

                    nameDataLength = rs.getInt("typlen")
                }
            }
        }

        return nameDataLength - 1
    }

    private fun getMaxIndexKeys(): Int {
        if (indexMaxKeys == 0) {
            val sql = "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"

            connection.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    if (!rs.next()) {
                        throw SQLException("Unable to determine a value for MaxIndexKeys due to missing system catalog data.")
                    }

                    indexMaxKeys = rs.getInt(1)
                }
            }
        }

        return indexMaxKeys
    }

    private fun createMetaDataStatement() = connection.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY
    )

    private fun PgwebResultSet.withUpperCaseColumnLabels() = PgwebResultSet(
        result.copy(columns = result.columns.map { it.uppercase() })
    )

    private fun getImportedExportedKeys(
        primaryCatalog: String?,
        primarySchema: String?,
        primaryTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet {
        val sql = """
            SELECT NULL::text AS PKTABLE_CAT, 
                   pkn.nspname AS PKTABLE_SCHEM, 
                   pkc.relname AS PKTABLE_NAME, 
                   pka.attname AS PKCOLUMN_NAME, 
                   NULL::text AS FKTABLE_CAT, 
                   fkn.nspname AS FKTABLE_SCHEM, 
                   fkc.relname AS FKTABLE_NAME, 
                   fka.attname AS FKCOLUMN_NAME, 
                   pos.n AS KEY_SEQ, 
                   CASE con.confupdtype 
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL 
                   END AS UPDATE_RULE, 
                   CASE con.confdeltype 
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL 
                   END AS DELETE_RULE, 
                   con.conname AS FK_NAME, pkic.relname AS PK_NAME, 
                   CASE 
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                   END AS DEFERRABILITY 
              FROM pg_catalog.pg_namespace pkn, 
                   pg_catalog.pg_class pkc, 
                   pg_catalog.pg_attribute pka, 
                   pg_catalog.pg_namespace fkn, 
                   pg_catalog.pg_class fkc, 
                   pg_catalog.pg_attribute fka, 
                   pg_catalog.pg_constraint con, 
                   pg_catalog.generate_series(1, ${getMaxIndexKeys()}) pos(n), 
                   pg_catalog.pg_class pkic, 
                   pg_catalog.pg_depend dep
             WHERE pkn.oid = pkc.relnamespace 
                   AND pkc.oid = pka.attrelid 
                   AND pka.attnum = con.confkey[pos.n] 
                   AND con.confrelid = pkc.oid 
                   AND fkn.oid = fkc.relnamespace 
                   AND fkc.oid = fka.attrelid 
                   AND fka.attnum = con.conkey[pos.n] 
                   AND con.conrelid = fkc.oid 
                   AND con.contype = 'f' 
                   AND (pkic.relkind = 'i' OR pkic.relkind = 'I')
                   AND pkic.oid = con.conindid
               ${("AND pkn.nspname = " + escapeQuotes(primarySchema)).takeIf { primarySchema?.isNotEmpty() == true } ?: ""}
               ${("AND fkn.nspname = " + escapeQuotes(foreignSchema)).takeIf { foreignSchema?.isNotEmpty() == true } ?: ""}
               ${("AND pkc.relname = " + escapeQuotes(primaryTable)).takeIf { primaryTable?.isNotEmpty() == true } ?: ""}
               ${("AND fkc.relname = " + escapeQuotes(foreignTable)).takeIf { foreignTable?.isNotEmpty() == true } ?: ""}
             ORDER BY ${if (primaryTable != null) "fkn.nspname,fkc.relname,con.conname,pos.n" else "pkn.nspname,pkc.relname, con.conname,pos.n"}
        """

        return createMetaDataStatement().executeQuery(sql)
    }
}
