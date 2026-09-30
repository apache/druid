/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.jdbc;

import org.apache.druid.jdbc.http.ColumnMetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Our implementation of JDBC {@link DatabaseMetaData}.
 */
public class DruidDatabaseMetaData implements DatabaseMetaData
{
  private static final String DEFAULT_DATABASE_PRODUCT_VERSION = "Unknown";
  private static final int NO_OR_UNKNOWN_LIMIT = 0;

  /**
   * Appended to each LIKE in a metadata query, matching {@link #getSearchStringEscape()}.
   */
  private static final String ESCAPE_CLAUSE = " ESCAPE '\\'";

  /**
   * Reserved words of Druid SQL that are not also reserved words of SQL:2003.
   */
  private static final String SQL_KEYWORDS =
      "ALLOW,ARRAY_MAX_CARDINALITY,BEGIN_FRAME,BEGIN_PARTITION,BIT,CLASSIFIER,CLUSTERED,CONTAINS,CURRENT_CATALOG," +
      "CURRENT_ROW,CURRENT_SCHEMA,DATETIME,DEFINE,DISALLOW,EMPTY,END_FRAME,END_PARTITION,EQUALS,EXPLAIN,EXTEND," +
      "FIRST_VALUE,FRAME_ROW,FRIDAY,GROUPS,INITIAL,JSON_ARRAY,JSON_ARRAYAGG,JSON_EXISTS,JSON_OBJECT,JSON_OBJECTAGG," +
      "JSON_QUERY,JSON_SCOPE,JSON_VALUE,LAG,LAST_VALUE,LEAD,LIKE_REGEX,LIMIT,MATCHES,MATCH_NUMBER,MATCH_RECOGNIZE," +
      "MEASURES,MINUS,MONDAY,NEXT,NTH_VALUE,NTILE,OCCURRENCES_REGEX,OFFSET,OMIT,ONE,ORDINAL,PARTITIONED,PATTERN,PER," +
      "PERCENT,PERIOD,PERMUTE,PORTION,POSITION_REGEX,PRECEDES,PREV,QUALIFY,RESET,RUNNING,SAFE_CAST,SAFE_OFFSET," +
      "SAFE_ORDINAL,SATURDAY,SEEK,SHOW,SKIP,STREAM,SUBSET,SUBSTRING_REGEX,SUCCEEDS,SUNDAY,SYSTEM_TIME,THURSDAY," +
      "TINYINT,TRANSLATE_REGEX,TRIM_ARRAY,TRUNCATE,TRY_CAST,TUESDAY,UPSERT,VALUE_OF,VARBINARY,VERSIONING,WEDNESDAY";

  /**
   * Numeric functions usable in a JDBC function escape, i.e. <code>{fn name(...)}</code>. Implemented in
   * Druid SQL on the server side, so the driver just needs to report the correct list.
   */
  private static final String NUMERIC_FUNCTIONS =
      "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,ROUND,SIN," +
      "SQRT,TAN,TRUNCATE";

  /**
   * String functions usable in a JDBC function escape, as {@link #NUMERIC_FUNCTIONS}.
   */
  private static final String STRING_FUNCTIONS =
      "CONCAT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,REPEAT,REPLACE,RIGHT,RTRIM,SUBSTRING,UCASE";

  /**
   * System functions usable in a JDBC function escape, as {@link #NUMERIC_FUNCTIONS}.
   */
  private static final String SYSTEM_FUNCTIONS = "CONVERT,IFNULL";

  /**
   * Time and date functions usable in a JDBC function escape, as {@link #NUMERIC_FUNCTIONS}.
   */
  private static final String TIME_DATE_FUNCTIONS =
      "CURDATE,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH,NOW,QUARTER,SECOND,TIMESTAMPADD," +
      "TIMESTAMPDIFF,WEEK,YEAR";

  /**
   * Columns of {@link #getImportedKeys}, {@link #getExportedKeys}, and {@link #getCrossReference}.
   */
  private static final List<ColumnMetadata> FOREIGN_KEY_COLUMNS = List.of(
      new ColumnMetadata("PKTABLE_CAT", JDBCType.VARCHAR),
      new ColumnMetadata("PKTABLE_SCHEM", JDBCType.VARCHAR),
      new ColumnMetadata("PKTABLE_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("PKCOLUMN_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("FKTABLE_CAT", JDBCType.VARCHAR),
      new ColumnMetadata("FKTABLE_SCHEM", JDBCType.VARCHAR),
      new ColumnMetadata("FKTABLE_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("FKCOLUMN_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("KEY_SEQ", JDBCType.INTEGER),
      new ColumnMetadata("UPDATE_RULE", JDBCType.INTEGER),
      new ColumnMetadata("DELETE_RULE", JDBCType.INTEGER),
      new ColumnMetadata("FK_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("PK_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("DEFERRABILITY", JDBCType.INTEGER)
  );

  /**
   * Columns of {@link #getBestRowIdentifier} and {@link #getVersionColumns}.
   */
  private static final List<ColumnMetadata> ROW_IDENTIFIER_COLUMNS = List.of(
      new ColumnMetadata("SCOPE", JDBCType.INTEGER),
      new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
      new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
      new ColumnMetadata("COLUMN_SIZE", JDBCType.INTEGER),
      new ColumnMetadata("BUFFER_LENGTH", JDBCType.INTEGER),
      new ColumnMetadata("DECIMAL_DIGITS", JDBCType.INTEGER),
      new ColumnMetadata("PSEUDO_COLUMN", JDBCType.INTEGER)
  );

  private final DruidConnection connection;
  private final Object databaseProductVersionLock = new Object();

  // Access guarded by databaseProductVersionLock.
  private String databaseProductVersion;

  public DruidDatabaseMetaData(final DruidConnection connection)
  {
    this.connection = connection;
  }

  @Override
  public boolean allProceduresAreCallable()
  {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable()
  {
    return true;
  }

  @Override
  public String getURL()
  {
    return "jdbc:druid:" + connection.getConnectionUrl().buildHttpUrl();
  }

  @Override
  public String getUserName()
  {
    final String user = connection.getConnectionUrl().getClientProperties().getUser();
    return user != null ? user : "";
  }

  @Override
  public boolean isReadOnly()
  {
    return true;
  }

  @Override
  public boolean nullsAreSortedHigh()
  {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow()
  {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart()
  {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd()
  {
    return false;
  }

  @Override
  public String getDatabaseProductName()
  {
    return "Apache Druid";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException
  {
    synchronized (databaseProductVersionLock) {
      if (databaseProductVersion == null) {
        databaseProductVersion = fetchDatabaseProductVersion();
      }
      return databaseProductVersion;
    }
  }

  @Override
  public String getDriverName()
  {
    return "Druid JDBC Driver";
  }

  @Override
  public String getDriverVersion()
  {
    return DruidJdbcDriver.getVersion();
  }

  @Override
  public int getDriverMajorVersion()
  {
    return DruidJdbcDriver.getStaticMajorVersion();
  }

  @Override
  public int getDriverMinorVersion()
  {
    return DruidJdbcDriver.getStaticMinorVersion();
  }

  @Override
  public boolean usesLocalFiles()
  {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable()
  {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers()
  {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers()
  {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers()
  {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers()
  {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers()
  {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers()
  {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers()
  {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers()
  {
    return false;
  }

  @Override
  public String getIdentifierQuoteString()
  {
    return "\"";
  }

  @Override
  public String getSQLKeywords()
  {
    return SQL_KEYWORDS;
  }

  @Override
  public String getNumericFunctions()
  {
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions()
  {
    return STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions()
  {
    return SYSTEM_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions()
  {
    return TIME_DATE_FUNCTIONS;
  }

  @Override
  public String getSearchStringEscape()
  {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters()
  {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn()
  {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn()
  {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing()
  {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull()
  {
    return true;
  }

  @Override
  public boolean supportsConvert()
  {
    return false;
  }

  @Override
  public boolean supportsConvert(final int fromType, final int toType)
  {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames()
  {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames()
  {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy()
  {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated()
  {
    return true;
  }

  @Override
  public boolean supportsGroupBy()
  {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated()
  {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect()
  {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause()
  {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets()
  {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions()
  {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns()
  {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar()
  {
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar()
  {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar()
  {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL()
  {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL()
  {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL()
  {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility()
  {
    return false;
  }

  @Override
  public boolean supportsOuterJoins()
  {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins()
  {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins()
  {
    return true;
  }

  @Override
  public String getSchemaTerm()
  {
    return "schema";
  }

  @Override
  public String getProcedureTerm()
  {
    return "procedure";
  }

  @Override
  public String getCatalogTerm()
  {
    return "catalog";
  }

  @Override
  public boolean isCatalogAtStart()
  {
    return true;
  }

  @Override
  public String getCatalogSeparator()
  {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation()
  {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls()
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions()
  {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions()
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions()
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation()
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls()
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions()
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions()
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions()
  {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete()
  {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate()
  {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate()
  {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures()
  {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons()
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists()
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns()
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds()
  {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries()
  {
    return true;
  }

  @Override
  public boolean supportsUnion()
  {
    return true;
  }

  @Override
  public boolean supportsUnionAll()
  {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit()
  {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback()
  {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit()
  {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback()
  {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxCharLiteralLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnsInGroupBy()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnsInIndex()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnsInOrderBy()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnsInSelect()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxColumnsInTable()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxConnections()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxCursorNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxIndexLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxSchemaNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxProcedureNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxCatalogNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxRowSize()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs()
  {
    return false;
  }

  @Override
  public int getMaxStatementLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxStatements()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxTableNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxTablesInSelect()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getMaxUserNameLength()
  {
    return NO_OR_UNKNOWN_LIMIT;
  }

  @Override
  public int getDefaultTransactionIsolation()
  {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsTransactions()
  {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(final int level)
  {
    return level == Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
  {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly()
  {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit()
  {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions()
  {
    return false;
  }

  @Override
  public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("PROCEDURE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("PROCEDURE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("PROCEDURE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("reserved1", JDBCType.VARCHAR),
        new ColumnMetadata("reserved2", JDBCType.VARCHAR),
        new ColumnMetadata("reserved3", JDBCType.VARCHAR),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("PROCEDURE_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("SPECIFIC_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getProcedureColumns(
      final String catalog,
      final String schemaPattern,
      final String procedureNamePattern,
      final String columnNamePattern
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("PROCEDURE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("PROCEDURE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("PROCEDURE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("PRECISION", JDBCType.INTEGER),
        new ColumnMetadata("LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("SCALE", JDBCType.INTEGER),
        new ColumnMetadata("RADIX", JDBCType.INTEGER),
        new ColumnMetadata("NULLABLE", JDBCType.INTEGER),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_DEF", JDBCType.VARCHAR),
        new ColumnMetadata("SQL_DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("SQL_DATETIME_SUB", JDBCType.INTEGER),
        new ColumnMetadata("CHAR_OCTET_LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("ORDINAL_POSITION", JDBCType.INTEGER),
        new ColumnMetadata("IS_NULLABLE", JDBCType.VARCHAR),
        new ColumnMetadata("SPECIFIC_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getTables(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String[] types
  ) throws SQLException
  {
    final List<String> whereBuilder = new ArrayList<>();
    final List<String> parameters = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("TABLES.TABLE_CATALOG = ?");
      parameters.add(catalog);
    }

    if (schemaPattern != null) {
      whereBuilder.add("TABLES.TABLE_SCHEMA LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(schemaPattern);
    }

    if (tableNamePattern != null) {
      whereBuilder.add("TABLES.TABLE_NAME LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(tableNamePattern);
    }

    if (types != null) {
      if (types.length == 0) {
        // An explicit but empty type list matches no tables.
        whereBuilder.add("1 = 0");
      } else {
        whereBuilder.add(
            "TABLES.TABLE_TYPE IN (" + String.join(", ", Collections.nCopies(types.length, "?")) + ")"
        );
        parameters.addAll(Arrays.asList(types));
      }
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + String.join(" AND ", whereBuilder);
    final String sql = """
                           SELECT
                             TABLE_CATALOG AS TABLE_CAT,
                             TABLE_SCHEMA AS TABLE_SCHEM,
                             TABLE_NAME AS TABLE_NAME,
                             TABLE_TYPE AS TABLE_TYPE,
                             CAST(NULL AS VARCHAR) AS REMARKS,
                             CAST(NULL AS VARCHAR) AS TYPE_CAT,
                             CAST(NULL AS VARCHAR) AS TYPE_SCHEM,
                             CAST(NULL AS VARCHAR) AS TYPE_NAME,
                             CAST(NULL AS VARCHAR) AS SELF_REFERENCING_COL_NAME,
                             CAST(NULL AS VARCHAR) AS REF_GENERATION
                           FROM
                             INFORMATION_SCHEMA.TABLES
                           """ + where + """
                           
                           ORDER BY
                             TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
                           """;

    return sqlResultSet(sql, parameters);
  }

  @Override
  public ResultSet getSchemas() throws SQLException
  {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException
  {
    final String sql = """
        SELECT
          DISTINCT CATALOG_NAME AS TABLE_CAT
        FROM
          INFORMATION_SCHEMA.SCHEMATA
        ORDER BY
          TABLE_CAT
        """;

    return sqlResultSet(sql, List.of());
  }

  @Override
  public ResultSet getTableTypes() throws SQLException
  {
    final String sql = """
        SELECT
          DISTINCT TABLE_TYPE AS TABLE_TYPE
        FROM
          INFORMATION_SCHEMA.TABLES
        ORDER BY
          TABLE_TYPE
        """;

    return sqlResultSet(sql, List.of());
  }

  @Override
  public ResultSet getColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern
  ) throws SQLException
  {
    final List<String> whereBuilder = new ArrayList<>();
    final List<String> parameters = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("COLUMNS.TABLE_CATALOG = ?");
      parameters.add(catalog);
    }

    if (schemaPattern != null) {
      whereBuilder.add("COLUMNS.TABLE_SCHEMA LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(schemaPattern);
    }

    if (tableNamePattern != null) {
      whereBuilder.add("COLUMNS.TABLE_NAME LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(tableNamePattern);
    }

    if (columnNamePattern != null) {
      whereBuilder.add("COLUMNS.COLUMN_NAME LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(columnNamePattern);
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + String.join(" AND ", whereBuilder);
    final String sql = """
                           SELECT
                             TABLE_CATALOG AS TABLE_CAT,
                             TABLE_SCHEMA AS TABLE_SCHEM,
                             TABLE_NAME AS TABLE_NAME,
                             COLUMN_NAME AS COLUMN_NAME,
                             CAST(JDBC_TYPE AS INTEGER) AS DATA_TYPE,
                             DATA_TYPE AS TYPE_NAME,
                             COALESCE(CAST(CHARACTER_MAXIMUM_LENGTH AS INTEGER), CAST(NUMERIC_PRECISION AS INTEGER)) AS COLUMN_SIZE,
                             -1 AS BUFFER_LENGTH,
                             CAST(NUMERIC_SCALE AS INTEGER) AS DECIMAL_DIGITS,
                             CAST(NUMERIC_PRECISION_RADIX AS INTEGER) AS NUM_PREC_RADIX,
                             CASE IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS NULLABLE,
                             CAST(NULL AS VARCHAR) AS REMARKS,
                             COLUMN_DEFAULT AS COLUMN_DEF,
                             -1 AS SQL_DATA_TYPE,
                             -1 AS SQL_DATETIME_SUB,
                             -1 AS CHAR_OCTET_LENGTH,
                             CAST(ORDINAL_POSITION AS INTEGER) AS ORDINAL_POSITION,
                             IS_NULLABLE AS IS_NULLABLE,
                             CAST(NULL AS VARCHAR) AS SCOPE_CATALOG,
                             CAST(NULL AS VARCHAR) AS SCOPE_SCHEMA,
                             CAST(NULL AS VARCHAR) AS SCOPE_TABLE,
                             -1 AS SOURCE_DATA_TYPE,
                             'NO' AS IS_AUTOINCREMENT,
                             'NO' AS IS_GENERATEDCOLUMN
                           FROM
                             INFORMATION_SCHEMA.COLUMNS
                           """ + where + """
                           
                           ORDER BY
                             TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
                           """;

    return sqlResultSet(sql, parameters);
  }

  @Override
  public ResultSet getColumnPrivileges(
      final String catalog,
      final String schema,
      final String table,
      final String columnNamePattern
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("GRANTOR", JDBCType.VARCHAR),
        new ColumnMetadata("GRANTEE", JDBCType.VARCHAR),
        new ColumnMetadata("PRIVILEGE", JDBCType.VARCHAR),
        new ColumnMetadata("IS_GRANTABLE", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("GRANTOR", JDBCType.VARCHAR),
        new ColumnMetadata("GRANTEE", JDBCType.VARCHAR),
        new ColumnMetadata("PRIVILEGE", JDBCType.VARCHAR),
        new ColumnMetadata("IS_GRANTABLE", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getBestRowIdentifier(
      final String catalog,
      final String schema,
      final String table,
      final int scope,
      final boolean nullable
  )
  {
    return DruidResultSet.createEmpty(ROW_IDENTIFIER_COLUMNS);
  }

  @Override
  public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
  {
    return DruidResultSet.createEmpty(ROW_IDENTIFIER_COLUMNS);
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("KEY_SEQ", JDBCType.INTEGER),
        new ColumnMetadata("PK_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
  {
    return DruidResultSet.createEmpty(FOREIGN_KEY_COLUMNS);
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
  {
    return DruidResultSet.createEmpty(FOREIGN_KEY_COLUMNS);
  }

  @Override
  public ResultSet getCrossReference(
      final String parentCatalog,
      final String parentSchema,
      final String parentTable,
      final String foreignCatalog,
      final String foreignSchema,
      final String foreignTable
  )
  {
    return DruidResultSet.createEmpty(FOREIGN_KEY_COLUMNS);
  }

  @Override
  public ResultSet getTypeInfo()
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("PRECISION", JDBCType.INTEGER),
        new ColumnMetadata("LITERAL_PREFIX", JDBCType.VARCHAR),
        new ColumnMetadata("LITERAL_SUFFIX", JDBCType.VARCHAR),
        new ColumnMetadata("CREATE_PARAMS", JDBCType.VARCHAR),
        new ColumnMetadata("NULLABLE", JDBCType.INTEGER),
        new ColumnMetadata("CASE_SENSITIVE", JDBCType.BOOLEAN),
        new ColumnMetadata("SEARCHABLE", JDBCType.INTEGER),
        new ColumnMetadata("UNSIGNED_ATTRIBUTE", JDBCType.BOOLEAN),
        new ColumnMetadata("FIXED_PREC_SCALE", JDBCType.BOOLEAN),
        new ColumnMetadata("AUTO_INCREMENT", JDBCType.BOOLEAN),
        new ColumnMetadata("LOCAL_TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("MINIMUM_SCALE", JDBCType.INTEGER),
        new ColumnMetadata("MAXIMUM_SCALE", JDBCType.INTEGER),
        new ColumnMetadata("SQL_DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("SQL_DATETIME_SUB", JDBCType.INTEGER),
        new ColumnMetadata("NUM_PREC_RADIX", JDBCType.INTEGER)
    ));
  }

  @Override
  public ResultSet getIndexInfo(
      final String catalog,
      final String schema,
      final String table,
      final boolean unique,
      final boolean approximate
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("NON_UNIQUE", JDBCType.BOOLEAN),
        new ColumnMetadata("INDEX_QUALIFIER", JDBCType.VARCHAR),
        new ColumnMetadata("INDEX_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE", JDBCType.INTEGER),
        new ColumnMetadata("ORDINAL_POSITION", JDBCType.INTEGER),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("ASC_OR_DESC", JDBCType.VARCHAR),
        new ColumnMetadata("CARDINALITY", JDBCType.BIGINT),
        new ColumnMetadata("PAGES", JDBCType.BIGINT),
        new ColumnMetadata("FILTER_CONDITION", JDBCType.VARCHAR)
    ));
  }

  @Override
  public boolean supportsResultSetType(final int type)
  {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(final int type, final int concurrency)
  {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(final int type)
  {
    return false;
  }

  @Override
  public boolean updatesAreDetected(final int type)
  {
    return false;
  }

  @Override
  public boolean deletesAreDetected(final int type)
  {
    return false;
  }

  @Override
  public boolean insertsAreDetected(final int type)
  {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates()
  {
    return false;
  }

  @Override
  public ResultSet getUDTs(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final int[] types
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TYPE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("CLASS_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("BASE_TYPE", JDBCType.INTEGER)
    ));
  }

  @Override
  public Connection getConnection()
  {
    return connection;
  }

  @Override
  public boolean supportsSavepoints()
  {
    return false;
  }

  @Override
  public boolean supportsNamedParameters()
  {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults()
  {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys()
  {
    return false;
  }

  @Override
  public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern)
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TYPE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("SUPERTYPE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("SUPERTYPE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("SUPERTYPE_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("SUPERTABLE_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getAttributes(
      final String catalog,
      final String schemaPattern,
      final String typeNamePattern,
      final String attributeNamePattern
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TYPE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("ATTR_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("ATTR_TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("ATTR_SIZE", JDBCType.INTEGER),
        new ColumnMetadata("DECIMAL_DIGITS", JDBCType.INTEGER),
        new ColumnMetadata("NUM_PREC_RADIX", JDBCType.INTEGER),
        new ColumnMetadata("NULLABLE", JDBCType.INTEGER),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("ATTR_DEF", JDBCType.VARCHAR),
        new ColumnMetadata("SQL_DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("SQL_DATETIME_SUB", JDBCType.INTEGER),
        new ColumnMetadata("CHAR_OCTET_LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("ORDINAL_POSITION", JDBCType.INTEGER),
        new ColumnMetadata("IS_NULLABLE", JDBCType.VARCHAR),
        new ColumnMetadata("SCOPE_CATALOG", JDBCType.VARCHAR),
        new ColumnMetadata("SCOPE_SCHEMA", JDBCType.VARCHAR),
        new ColumnMetadata("SCOPE_TABLE", JDBCType.VARCHAR),
        new ColumnMetadata("SOURCE_DATA_TYPE", JDBCType.INTEGER)
    ));
  }

  @Override
  public boolean supportsResultSetHoldability(final int holdability)
  {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability()
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException
  {
    return parseVersionPart(0);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException
  {
    return parseVersionPart(1);
  }

  @Override
  public int getJDBCMajorVersion()
  {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion()
  {
    return 2;
  }

  @Override
  public int getSQLStateType()
  {
    return DatabaseMetaData.sqlStateSQL99;
  }

  @Override
  public boolean locatorsUpdateCopy()
  {
    return false;
  }

  @Override
  public boolean supportsStatementPooling()
  {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime()
  {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException
  {
    final List<String> whereBuilder = new ArrayList<>();
    final List<String> parameters = new ArrayList<>();
    if (catalog != null) {
      whereBuilder.add("SCHEMATA.CATALOG_NAME = ?");
      parameters.add(catalog);
    }

    if (schemaPattern != null) {
      whereBuilder.add("SCHEMATA.SCHEMA_NAME LIKE ?" + ESCAPE_CLAUSE);
      parameters.add(schemaPattern);
    }

    final String where = whereBuilder.isEmpty() ? "" : "WHERE " + String.join(" AND ", whereBuilder);
    final String sql = """
                           SELECT
                             SCHEMA_NAME AS TABLE_SCHEM,
                             CATALOG_NAME AS TABLE_CATALOG
                           FROM
                             INFORMATION_SCHEMA.SCHEMATA
                           """ + where + """
                           
                           ORDER BY
                             TABLE_CATALOG, TABLE_SCHEM
                           """;

    return sqlResultSet(sql, parameters);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax()
  {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets()
  {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties()
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("NAME", JDBCType.VARCHAR),
        new ColumnMetadata("MAX_LEN", JDBCType.INTEGER),
        new ColumnMetadata("DEFAULT_VALUE", JDBCType.VARCHAR),
        new ColumnMetadata("DESCRIPTION", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)

  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("FUNCTION_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("FUNCTION_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("FUNCTION_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("FUNCTION_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("SPECIFIC_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getFunctionColumns(
      final String catalog,
      final String schemaPattern,
      final String functionNamePattern,
      final String columnNamePattern
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("FUNCTION_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("FUNCTION_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("FUNCTION_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("TYPE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("PRECISION", JDBCType.INTEGER),
        new ColumnMetadata("LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("SCALE", JDBCType.INTEGER),
        new ColumnMetadata("RADIX", JDBCType.INTEGER),
        new ColumnMetadata("NULLABLE", JDBCType.INTEGER),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("CHAR_OCTET_LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("ORDINAL_POSITION", JDBCType.INTEGER),
        new ColumnMetadata("IS_NULLABLE", JDBCType.VARCHAR),
        new ColumnMetadata("SPECIFIC_NAME", JDBCType.VARCHAR)
    ));
  }

  @Override
  public ResultSet getPseudoColumns(
      final String catalog,
      final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern
  )
  {
    return DruidResultSet.createEmpty(List.of(
        new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR),
        new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
        new ColumnMetadata("DATA_TYPE", JDBCType.INTEGER),
        new ColumnMetadata("COLUMN_SIZE", JDBCType.INTEGER),
        new ColumnMetadata("DECIMAL_DIGITS", JDBCType.INTEGER),
        new ColumnMetadata("NUM_PREC_RADIX", JDBCType.INTEGER),
        new ColumnMetadata("COLUMN_USAGE", JDBCType.VARCHAR),
        new ColumnMetadata("REMARKS", JDBCType.VARCHAR),
        new ColumnMetadata("CHAR_OCTET_LENGTH", JDBCType.INTEGER),
        new ColumnMetadata("IS_NULLABLE", JDBCType.VARCHAR)
    ));
  }

  @Override
  public boolean generatedKeyAlwaysReturned()
  {
    return false;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException
  {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new DruidJdbcException("Cannot unwrap to class[%s]", iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface)
  {
    return iface.isAssignableFrom(getClass());
  }

  /**
   * Issue a query to the server and return the response as a ResultSet.
   */
  private ResultSet sqlResultSet(final String sql, final List<String> parameters) throws SQLException
  {
    final PreparedStatement statement = connection.prepareStatement(sql);
    statement.closeOnCompletion();
    try {
      for (int i = 0; i < parameters.size(); i++) {
        statement.setString(i + 1, parameters.get(i));
      }
      return statement.executeQuery();
    }
    catch (Throwable e) {
      // No ResultSet was produced, so closeOnCompletion() will never fire.
      try {
        statement.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }
      throw new DruidJdbcException(e, "Failed to execute metadata query");
    }
  }

  /**
   * Parses the dot-separated segment at the given index from {@link #getDatabaseProductVersion()}, returning 0 if it
   * is missing or not numeric.
   */
  private int parseVersionPart(final int index) throws SQLException
  {
    final String[] parts = getDatabaseProductVersion().split("\\.");
    if (index >= parts.length) {
      return 0;
    }
    try {
      return Integer.parseInt(parts[index]);
    }
    catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Reads the server version from {@code sys.servers}, or returns {@link #DEFAULT_DATABASE_PRODUCT_VERSION} if it
   * cannot be read.
   */
  private String fetchDatabaseProductVersion() throws SQLException
  {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet rs = stmt.executeQuery("SELECT \"version\" FROM sys.servers LIMIT 1")) {
        if (rs.next()) {
          final String version = rs.getString(1);
          if (version != null && !version.isEmpty()) {
            return version;
          }
        }
      }
    }
    catch (SQLException e) {
      if (DruidSQLState.InvalidAuthorizationSpecification.getSqlState().equals(e.getSQLState())) {
        // Fall through to return the default version if the query to sys.servers is forbidden.
      } else {
        throw e;
      }
    }
    return DEFAULT_DATABASE_PRODUCT_VERSION;
  }
}
