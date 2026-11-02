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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.http.ColumnMetadata;
import org.apache.druid.jdbc.http.TestQueryResultsIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DruidDatabaseMetaDataTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final String JDBC_URL = "jdbc:druid:http://localhost:8888/druid/v2/sql/";

  /**
   * The statement is mocked so that the tests can read the SQL that DruidDatabaseMetaData generated, before a
   * real DruidPreparedStatement would rewrite it on the way to the server.
   */
  @Mock
  private PreparedStatement mockStatement;

  @Mock
  private DruidConnection mockConnection;

  private DruidDatabaseMetaData metaData;

  @BeforeEach
  public void setUp() throws SQLException
  {
    lenient().when(mockConnection.getConnectionUrl()).thenReturn(DruidConnectionUrl.parse(JDBC_URL, null));
    lenient().when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    metaData = new DruidDatabaseMetaData(mockConnection);
  }

  /**
   * Stubs the response the server would return for the next metadata query.
   */
  private void stubResults(final List<ColumnMetadata> columns, final List<Object[]> rows) throws SQLException
  {
    when(mockStatement.executeQuery())
        .thenReturn(new DruidResultSet(new TestQueryResultsIterator(columns, rows), null, JSON_MAPPER));
  }

  /**
   * SQL that {@link #metaData} sent to the server, which is where server-side filters are observable.
   */
  private String executedSql() throws SQLException
  {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockConnection).prepareStatement(sqlCaptor.capture());
    return sqlCaptor.getValue();
  }

  /**
   * Values bound to the placeholders of {@link #executedSql()}, in order.
   */
  private List<String> boundParameters() throws SQLException
  {
    final ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockStatement, Mockito.atLeast(0)).setString(anyInt(), valueCaptor.capture());
    return valueCaptor.getAllValues();
  }

  private static List<String> columnValues(final ResultSet rs, final String columnLabel) throws SQLException
  {
    final List<String> values = new ArrayList<>();
    while (rs.next()) {
      values.add(rs.getString(columnLabel));
    }
    return values;
  }

  @Test
  public void testFunctionSupport()
  {
    MatcherAssert.assertThat(metaData.getNumericFunctions(), Matchers.containsString("ABS"));
    MatcherAssert.assertThat(metaData.getNumericFunctions(), Matchers.containsString("LOG10"));
    MatcherAssert.assertThat(metaData.getStringFunctions(), Matchers.containsString("CONCAT"));
    MatcherAssert.assertThat(metaData.getStringFunctions(), Matchers.containsString("LCASE"));
    MatcherAssert.assertThat(metaData.getTimeDateFunctions(), Matchers.containsString("CURDATE"));
    MatcherAssert.assertThat(metaData.getTimeDateFunctions(), Matchers.containsString("NOW"));
    MatcherAssert.assertThat(metaData.getTimeDateFunctions(), Matchers.containsString("TIMESTAMPADD"));
    MatcherAssert.assertThat(metaData.getSystemFunctions(), Matchers.containsString("IFNULL"));

    // Functions in Calcite's JDBC lists that Druid cannot plan are deliberately not advertised.
    MatcherAssert.assertThat(metaData.getNumericFunctions(), Matchers.not(Matchers.containsString("RAND")));
    MatcherAssert.assertThat(metaData.getStringFunctions(), Matchers.not(Matchers.containsString("SOUNDEX")));
    MatcherAssert.assertThat(metaData.getTimeDateFunctions(), Matchers.not(Matchers.containsString("CURTIME")));
    MatcherAssert.assertThat(metaData.getSystemFunctions(), Matchers.not(Matchers.containsString("USER")));
  }

  @Test
  public void testGetTableTypes() throws SQLException
  {
    stubResults(
        List.of(new ColumnMetadata("TABLE_TYPE", JDBCType.VARCHAR)),
        List.of(new Object[]{"TABLE"}, new Object[]{"VIEW"})
    );

    Assertions.assertEquals(List.of("TABLE", "VIEW"), columnValues(metaData.getTableTypes(), "TABLE_TYPE"));

    Assertions.assertEquals(
        """
        SELECT
          DISTINCT TABLE_TYPE AS TABLE_TYPE
        FROM
          INFORMATION_SCHEMA.TABLES
        ORDER BY
          TABLE_TYPE
        """,
        executedSql()
    );
  }

  /**
   * The driver does not describe its types, so this is empty but carries the columns the spec requires.
   */
  @Test
  public void testGetTypeInfo() throws SQLException
  {
    final ResultSet rs = metaData.getTypeInfo();

    final ResultSetMetaData rsMetaData = rs.getMetaData();
    Assertions.assertEquals(18, rsMetaData.getColumnCount());
    Assertions.assertEquals("TYPE_NAME", rsMetaData.getColumnName(1));
    Assertions.assertEquals("DATA_TYPE", rsMetaData.getColumnName(2));
    Assertions.assertEquals("PRECISION", rsMetaData.getColumnName(3));
    Assertions.assertEquals("NUM_PREC_RADIX", rsMetaData.getColumnName(18));

    Assertions.assertFalse(rs.next());
  }

  @Test
  public void testGetTables() throws SQLException
  {
    stubResults(
        List.of(new ColumnMetadata("TABLE_NAME", JDBCType.VARCHAR)),
        List.of(new Object[]{"test_table"}, new Object[]{"test_view"})
    );

    final ResultSet rs = metaData.getTables(null, null, null, null);
    Assertions.assertEquals(List.of("test_table", "test_view"), columnValues(rs, "TABLE_NAME"));

    Assertions.assertEquals(
        """
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

        ORDER BY
          TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
        """,
        executedSql()
    );
  }

  @Test
  public void testGetSchemas() throws SQLException
  {
    stubResults(
        List.of(new ColumnMetadata("TABLE_SCHEM", JDBCType.VARCHAR)),
        List.of(new Object[]{"druid"}, new Object[]{"sys"})
    );

    Assertions.assertEquals(List.of("druid", "sys"), columnValues(metaData.getSchemas(), "TABLE_SCHEM"));

    Assertions.assertEquals(
        """
        SELECT
          SCHEMA_NAME AS TABLE_SCHEM,
          CATALOG_NAME AS TABLE_CATALOG
        FROM
          INFORMATION_SCHEMA.SCHEMATA

        ORDER BY
          TABLE_CATALOG, TABLE_SCHEM
        """,
        executedSql()
    );
  }

  @Test
  public void testGetColumns() throws SQLException
  {
    // Only the columns this test reads. getColumns aliases everything server-side and passes the rows through.
    stubResults(
        List.of(
            new ColumnMetadata("COLUMN_NAME", JDBCType.VARCHAR),
            new ColumnMetadata("COLUMN_SIZE", JDBCType.INTEGER),
            new ColumnMetadata("DECIMAL_DIGITS", JDBCType.INTEGER),
            new ColumnMetadata("NUM_PREC_RADIX", JDBCType.INTEGER)
        ),
        List.of(
            new Object[]{"id", 19, 0, 10},
            new Object[]{"name", 1024, -1, -1},
            new Object[]{"timestamp", -1, -1, -1}
        )
    );

    final ResultSet rs = metaData.getColumns(null, null, "test_table", null);

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals("id", rs.getString("COLUMN_NAME"));

    // Size metadata is passed through from INFORMATION_SCHEMA rather than hardcoded to -1.
    Assertions.assertEquals(19, rs.getInt("COLUMN_SIZE"));
    Assertions.assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
    Assertions.assertEquals(10, rs.getInt("NUM_PREC_RADIX"));

    Assertions.assertEquals(List.of("name", "timestamp"), columnValues(rs, "COLUMN_NAME"));

    Assertions.assertEquals(
        """
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
        WHERE COLUMNS.TABLE_NAME LIKE ? ESCAPE '\\'
        ORDER BY
          TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
        """,
        executedSql()
    );

    Assertions.assertEquals(List.of("test_table"), boundParameters());
  }

  @Test
  public void testGetURL()
  {
    Assertions.assertEquals(JDBC_URL, metaData.getURL());
  }

  @Test
  public void testGetUserName() throws SQLException
  {
    // An unset user reads as the empty string rather than null, which is what DatabaseMetaData specifies.
    Assertions.assertEquals("", metaData.getUserName());

    when(mockConnection.getConnectionUrl()).thenReturn(DruidConnectionUrl.parse(JDBC_URL + "?user=testuser", null));
    Assertions.assertEquals("testuser", metaData.getUserName());
  }

  @Test
  public void testGetCatalogs() throws SQLException
  {
    stubResults(
        List.of(new ColumnMetadata("TABLE_CAT", JDBCType.VARCHAR)),
        List.of(new Object[]{"druid"}, new Object[]{"sys"})
    );

    Assertions.assertEquals(List.of("druid", "sys"), columnValues(metaData.getCatalogs(), "TABLE_CAT"));

    Assertions.assertEquals(
        """
        SELECT
          DISTINCT CATALOG_NAME AS TABLE_CAT
        FROM
          INFORMATION_SCHEMA.SCHEMATA
        ORDER BY
          TABLE_CAT
        """,
        executedSql()
    );
  }

  /**
   * The version comes from sys.servers, is parsed into components, and is cached rather than re-queried.
   */
  @Test
  public void testGetDatabaseVersion(@Mock final Statement versionStatement) throws SQLException
  {
    final TestQueryResultsIterator results = new TestQueryResultsIterator(
        List.of(new ColumnMetadata("version", JDBCType.VARCHAR)),
        Collections.singletonList(new Object[]{"31.0.0"})
    );
    when(mockConnection.createStatement()).thenReturn(versionStatement);
    when(versionStatement.executeQuery(any(String.class)))
        .thenReturn(new DruidResultSet(results, null, JSON_MAPPER));

    Assertions.assertEquals("31.0.0", metaData.getDatabaseProductVersion());
    Assertions.assertEquals("31.0.0", metaData.getDatabaseProductVersion());
    Assertions.assertEquals(31, metaData.getDatabaseMajorVersion());
    Assertions.assertEquals(0, metaData.getDatabaseMinorVersion());

    Mockito.verify(mockConnection, Mockito.times(1)).createStatement();
    Mockito.verify(versionStatement).executeQuery("SELECT \"version\" FROM sys.servers LIMIT 1");
  }

  @Test
  public void testGetDatabaseVersionWithoutSysPermission(@Mock final Statement versionStatement) throws SQLException
  {
    when(mockConnection.createStatement()).thenReturn(versionStatement);
    when(versionStatement.executeQuery(any(String.class))).thenThrow(
        new DruidJdbcException(
            DruidSQLState.InvalidAuthorizationSpecification,
            "HTTP 403 error from[http://localhost:8888/druid/v2/sql/]: body: Unauthorized"
        )
    );

    Assertions.assertEquals("Unknown", metaData.getDatabaseProductVersion());
    Assertions.assertEquals(0, metaData.getDatabaseMajorVersion());
    Assertions.assertEquals(0, metaData.getDatabaseMinorVersion());

    // One attempt, however many version methods are called.
    Mockito.verify(mockConnection, Mockito.times(1)).createStatement();
  }

  /**
   * Filters are applied server-side, so they are only observable in the generated SQL. Caller-supplied values are
   * bound as parameters, which is what keeps SQL syntax in them from being interpreted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generatedSqlCases")
  public void testGeneratedSql(
      final MetadataQuery call,
      final List<String> expectedFragments,
      final List<String> expectedParameters
  ) throws SQLException
  {
    stubResults(List.of(new ColumnMetadata("dummy", JDBCType.VARCHAR)), List.of());
    call.run(metaData);

    final String sql = executedSql();
    for (final String fragment : expectedFragments) {
      Assertions.assertTrue(sql.contains(fragment), "Expected [" + fragment + "] in: " + sql);
    }

    Assertions.assertEquals(expectedParameters, boundParameters());
  }

  private static Stream<Arguments> generatedSqlCases()
  {
    return Stream.of(
        sqlCase(
            "getTables schema filter",
            md -> md.getTables(null, "druid", null, null),
            List.of("TABLES.TABLE_SCHEMA LIKE ? ESCAPE '\\'"),
            List.of("druid")
        ),
        sqlCase(
            "getTables type filter",
            md -> md.getTables(null, null, null, new String[]{"TABLE", "VIEW"}),
            List.of("TABLES.TABLE_TYPE IN (?, ?)"),
            List.of("TABLE", "VIEW")
        ),
        sqlCase(
            "getTables catalog, schema, and name filters",
            md -> md.getTables("druid", "schema%", "table_name", null),
            List.of("TABLES.TABLE_CATALOG = ?", "TABLES.TABLE_SCHEMA LIKE ?", "TABLES.TABLE_NAME LIKE ?"),
            List.of("druid", "schema%", "table_name")
        ),
        sqlCase(
            "getColumns column filter",
            md -> md.getColumns(null, "druid", "wikipedia", "__time"),
            List.of("COLUMNS.COLUMN_NAME LIKE ? ESCAPE '\\'"),
            List.of("druid", "wikipedia", "__time")
        ),
        // Values that would change the meaning of the SQL if they were spliced into it.
        sqlCase(
            "getColumns quotes and injection attempt",
            md -> md.getColumns("cat'", "schema'x", "' OR 1=1 --", "a''b"),
            List.of("INFORMATION_SCHEMA.COLUMNS"),
            List.of("cat'", "schema'x", "' OR 1=1 --", "a''b")
        ),
        sqlCase(
            "getSchemas injection attempt",
            md -> md.getSchemas("O'Reilly", "'; DELETE FROM sys.schemata WHERE '1'='1"),
            List.of("SCHEMATA.CATALOG_NAME = ?", "SCHEMATA.SCHEMA_NAME LIKE ?"),
            List.of("O'Reilly", "'; DELETE FROM sys.schemata WHERE '1'='1")
        )
    );
  }

  private static Arguments sqlCase(
      final String name,
      final MetadataQuery call,
      final List<String> expectedFragments,
      final List<String> expectedParameters
  )
  {
    return Arguments.of(Named.of(name, call), expectedFragments, expectedParameters);
  }

  @Test
  public void testGetTablesEmptyTypesArrayMatchesNoTables() throws SQLException
  {
    stubResults(List.of(new ColumnMetadata("dummy", JDBCType.VARCHAR)), List.of());
    metaData.getTables(null, null, null, new String[0]);

    final String sql = executedSql();
    Assertions.assertFalse(sql.contains("IN ()"), "Unexpected 'IN ()' clause in: " + sql);
    Assertions.assertTrue(sql.contains("1 = 0"), "Expected an always-false predicate in: " + sql);
  }

  @Test
  public void testMetadataQueryFailureClosesStatement() throws SQLException
  {
    when(mockStatement.executeQuery()).thenThrow(new DruidJdbcException("metadata query failed"));

    Assertions.assertThrows(SQLException.class, () -> metaData.getTables(null, null, null, null));

    // No ResultSet was returned, so close-on-completion cannot fire and the close must be explicit.
    Mockito.verify(mockStatement).close();
  }

  /**
   * A failure that is not already a SQLException is wrapped, unlike in testMetadataQueryFailureClosesStatement.
   */
  @Test
  public void testMetadataQueryWrapsRuntimeException() throws SQLException
  {
    when(mockStatement.executeQuery()).thenThrow(new RuntimeException("Connection failed"));

    assertWrapsRuntimeException(() -> metaData.getTables(null, null, null, null));
    assertWrapsRuntimeException(() -> metaData.getColumns(null, "druid", "test_table", null));
    assertWrapsRuntimeException(metaData::getSchemas);
  }

  private static void assertWrapsRuntimeException(final Executable call)
  {
    final SQLException exception = Assertions.assertThrows(SQLException.class, call);
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Failed to execute metadata query"));
  }

  @FunctionalInterface
  private interface MetadataQuery
  {
    void run(DruidDatabaseMetaData metaData) throws SQLException;
  }
}
