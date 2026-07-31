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

package org.apache.druid.jdbc.embedded;

import org.apache.druid.guice.SleepModule;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class EmbeddedJdbcDriverTest extends EmbeddedClusterTestBase
{
  private static final Instant NONZERO_TIME_OF_DAY = Instant.parse("2025-06-01T15:40:30.123Z");

  private static final String NONZERO_TIME_OF_DAY_QUERY =
      StringUtils.format("SELECT TIME_PARSE('%s') AS ts", NONZERO_TIME_OF_DAY);

  private static final DateTimeFormatter WALL_CLOCK_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);

  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(router)
                               .addExtension(SleepModule.class);
  }

  @BeforeEach
  void setUp()
  {
    runIndexTask();
  }

  @Test
  @Timeout(30)
  public void test_statement_selectCount() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      try (Statement statement = connection.createStatement()) {
        final String query = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          assertResultSet(Collections.singletonList(new Object[]{10L}), resultSet);
        }
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_statement_selectStarOrderByTimeLimit() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      try (Statement statement = connection.createStatement()) {
        final String query = StringUtils.format("SELECT * FROM \"%s\" ORDER BY __time LIMIT 5", dataSource);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          final List<Object[]> expectedResults = List.of(
              new Object[]{Timestamp.valueOf("2025-06-01 00:00:00"), "shirt", "105"},
              new Object[]{Timestamp.valueOf("2025-06-02 00:00:00"), "trousers", "210"},
              new Object[]{Timestamp.valueOf("2025-06-03 00:00:00"), "jeans", "150"},
              new Object[]{Timestamp.valueOf("2025-06-04 00:00:00"), "t-shirt", "53"},
              new Object[]{Timestamp.valueOf("2025-06-05 00:00:00"), "microwave", "1099"}
          );

          final ResultSetMetaData metaData = resultSet.getMetaData();
          Assertions.assertEquals(3, metaData.getColumnCount());
          Assertions.assertEquals("__time", metaData.getColumnName(1));
          Assertions.assertEquals("item", metaData.getColumnName(2));
          Assertions.assertEquals("value", metaData.getColumnName(3));
          Assertions.assertEquals("TIMESTAMP", metaData.getColumnTypeName(1));
          Assertions.assertEquals("VARCHAR", metaData.getColumnTypeName(2));
          Assertions.assertEquals("VARCHAR", metaData.getColumnTypeName(3));

          assertResultSet(expectedResults, resultSet);
        }
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_statement_jdbcEscapes() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Lower case, as clients emit it; resolves only because Druid's parser normalizes the function name.
        final String query = StringUtils.format(
            "SELECT {fn concat(item, '!')}, {fn abs(0 - CAST(\"value\" AS BIGINT))}, {fn ucase(item)} "
            + "FROM \"%s\" "
            + "WHERE __time = {ts '2025-06-01 00:00:00'} "
            + "AND {fn curdate()} > {d '2025-06-01'}"
            + "AND __time < {fn now()}",
            dataSource
        );
        try (ResultSet resultSet = statement.executeQuery(query)) {
          assertResultSet(Collections.singletonList(new Object[]{"shirt!", 105L, "SHIRT"}), resultSet);
        }
      }
    }
  }

  @Test
  @Timeout(60)
  public void test_preparedStatement_timestampParamWithSqlTimeZone() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("SET sqlTimeZone = 'America/Los_Angeles'");
      }
      try (final PreparedStatement statement =
               connection.prepareStatement("SELECT TIMESTAMP_TO_MILLIS(?), TIME_FORMAT(?), ?")) {
        final Timestamp expectedTimestamp = new Timestamp(1577836800000L); // 2020-01-01T00:00:00Z
        statement.setTimestamp(1, expectedTimestamp);
        statement.setTimestamp(2, expectedTimestamp);
        statement.setTimestamp(3, expectedTimestamp);
        try (ResultSet resultSet = statement.executeQuery()) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(1577836800000L, resultSet.getLong(1));
          Assertions.assertEquals("2019-12-31T16:00:00.000-08:00", resultSet.getString(2));
          Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(3));
        }
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_preparedStatement_withParameters() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      final String query = StringUtils.format("SELECT * FROM \"%s\" WHERE item = ? ORDER BY __time", dataSource);
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setString(1, "shirt");
        try (ResultSet resultSet = statement.executeQuery()) {
          assertResultSet(
              List.of(
                  new Object[]{Timestamp.valueOf("2025-06-01 00:00:00"), "shirt", "105"},
                  new Object[]{Timestamp.valueOf("2025-06-09 00:00:00"), "shirt", "99"}
              ),
              resultSet
          );
        }
      }

      final String multiParamQuery = StringUtils.format(
          "SELECT COUNT(*) FROM \"%s\" WHERE item = ? OR item = ?",
          dataSource
      );
      try (PreparedStatement statement = connection.prepareStatement(multiParamQuery)) {
        statement.setString(1, "shirt");
        statement.setString(2, "jeans");
        try (ResultSet resultSet = statement.executeQuery()) {
          // shirt (2 rows) + jeans (1 row).
          assertResultSet(Collections.singletonList(new Object[]{3L}), resultSet);
        }
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_preparedStatement_getMetaData() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      final String query = StringUtils.format("SELECT __time, item FROM \"%s\" WHERE item = ?", dataSource);
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setString(1, "shirt");

        final ResultSetMetaData metaData = statement.getMetaData();
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("__time", metaData.getColumnName(1));
        Assertions.assertEquals("TIMESTAMP", metaData.getColumnTypeName(1));
        Assertions.assertEquals("item", metaData.getColumnName(2));
        Assertions.assertEquals("VARCHAR", metaData.getColumnTypeName(2));
      }

      final String noParamQuery = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
      try (PreparedStatement statement = connection.prepareStatement(noParamQuery)) {
        final ResultSetMetaData metaData = statement.getMetaData();
        Assertions.assertEquals(1, metaData.getColumnCount());
        Assertions.assertEquals("EXPR$0", metaData.getColumnName(1));
        Assertions.assertEquals("BIGINT", metaData.getColumnTypeName(1));
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_preparedStatement_getMetaDataWithoutBoundParameters() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT ? AS c1, ? || 'x' AS c2")) {
        final ResultSetMetaData metaData = statement.getMetaData();
        Assertions.assertEquals(2, metaData.getColumnCount());

        // c1 type is reported as NULL due to being unbound.
        Assertions.assertEquals("c1", metaData.getColumnName(1));
        Assertions.assertEquals(Types.NULL, metaData.getColumnType(1));
        Assertions.assertEquals("NULL", metaData.getColumnTypeName(1));

        // c2 type is VARCHAR; parameter doesn't matter since the || operator always returns VARCHAR.
        Assertions.assertEquals("c2", metaData.getColumnName(2));
        Assertions.assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        Assertions.assertEquals("VARCHAR", metaData.getColumnTypeName(2));
      }
    }
  }

  @Test
  @Timeout(30)
  public void test_timestamp_methods_nonzeroTimeOfDay() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl());
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(NONZERO_TIME_OF_DAY_QUERY)) {
      Assertions.assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(1));
      Assertions.assertTrue(resultSet.next());

      // getTimestamp returns the instant itself, to millisecond precision.
      final Timestamp timestamp = resultSet.getTimestamp(1);
      Assertions.assertEquals(NONZERO_TIME_OF_DAY.toEpochMilli(), timestamp.getTime());
      Assertions.assertEquals(Timestamp.valueOf("2025-06-01 15:40:30.123"), timestamp); // Default time zone is UTC
      Assertions.assertFalse(resultSet.wasNull());

      // getDate truncates to midnight of the day the instant falls on, in the JVM default time zone.
      final Date date = resultSet.getDate(1);
      Assertions.assertEquals("2025-06-01 00:00:00.000", wallClock(date, ZoneOffset.UTC));
      Assertions.assertEquals(Date.valueOf("2025-06-01"), date);
      Assertions.assertFalse(resultSet.wasNull());

      // getTime keeps the time of day and moves it onto the epoch day. Time.valueOf has no sub-second
      // component, so compare the wall clock rather than a Time.
      final Time time = resultSet.getTime(1);
      Assertions.assertEquals("1970-01-01 15:40:30.123", wallClock(time, ZoneOffset.UTC));
      Assertions.assertFalse(resultSet.wasNull());

      Assertions.assertEquals(timestamp, resultSet.getTimestamp("ts"));
      Assertions.assertEquals(date, resultSet.getDate("ts"));
      Assertions.assertEquals(time, resultSet.getTime("ts"));

      Assertions.assertEquals(timestamp, resultSet.getObject(1, Timestamp.class));
      Assertions.assertEquals(date, resultSet.getObject(1, Date.class));
      Assertions.assertEquals(time, resultSet.getObject(1, Time.class));

      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  @Timeout(30)
  public void test_timestamp_methods_withCalendar() throws Exception
  {
    // 2025-06-01T15:40:30.123Z reads as 08:40:30.123 the same day in Los Angeles (PDT, UTC-7), and as
    // 21:10:30.123 the same day in Kolkata (IST, UTC+5:30).
    final ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
    final ZoneId kolkata = ZoneId.of("Asia/Kolkata");
    final Calendar losAngelesCal = Calendar.getInstance(TimeZone.getTimeZone(losAngeles), Locale.ENGLISH);
    final Calendar kolkataCal = Calendar.getInstance(TimeZone.getTimeZone(kolkata), Locale.ENGLISH);

    try (Connection connection = DriverManager.getConnection(getJdbcUrl());
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(NONZERO_TIME_OF_DAY_QUERY)) {
      Assertions.assertTrue(resultSet.next());

      // getTimestamp ignores the calendar: Druid's results carry a UTC offset, so the instant is unambiguous.
      final Timestamp expectedTimestamp = Timestamp.from(NONZERO_TIME_OF_DAY);
      Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1, losAngelesCal));
      Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1, kolkataCal));
      Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1, null));
      Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp("ts", losAngelesCal));

      // getDate returns midnight of the day the instant falls on, as read in the calendar's time zone.
      Assertions.assertEquals("2025-06-01 00:00:00.000", wallClock(resultSet.getDate(1, losAngelesCal), losAngeles));
      Assertions.assertEquals("2025-06-01 00:00:00.000", wallClock(resultSet.getDate(1, kolkataCal), kolkata));

      // Midnight of 2025-06-01 is 07:00Z in Los Angeles and 18:30Z the day before in Kolkata, so the same
      // value yields two Dates that are not equal.
      Assertions.assertEquals(
          Instant.parse("2025-06-01T07:00:00Z").toEpochMilli(),
          resultSet.getDate(1, losAngelesCal).getTime()
      );
      Assertions.assertEquals(
          Instant.parse("2025-05-31T18:30:00Z").toEpochMilli(),
          resultSet.getDate(1, kolkataCal).getTime()
      );
      Assertions.assertEquals(resultSet.getDate(1, losAngelesCal), resultSet.getDate("ts", losAngelesCal));

      // getTime returns the time of day as read in the calendar's time zone, placed on the epoch day.
      Assertions.assertEquals("1970-01-01 08:40:30.123", wallClock(resultSet.getTime(1, losAngelesCal), losAngeles));
      Assertions.assertEquals("1970-01-01 21:10:30.123", wallClock(resultSet.getTime(1, kolkataCal), kolkata));

      // Los Angeles was on PST (UTC-8) on the epoch day, not the PDT (UTC-7) that applies on 2025-06-01.
      Assertions.assertEquals(
          Instant.parse("1970-01-01T16:40:30.123Z").toEpochMilli(),
          resultSet.getTime(1, losAngelesCal).getTime()
      );
      Assertions.assertEquals(resultSet.getTime(1, losAngelesCal), resultSet.getTime("ts", losAngelesCal));

      // A null calendar means the JVM default time zone, which the tests set to UTC.
      Assertions.assertEquals("2025-06-01 00:00:00.000", wallClock(resultSet.getDate(1, null), ZoneOffset.UTC));
      Assertions.assertEquals("1970-01-01 15:40:30.123", wallClock(resultSet.getTime(1, null), ZoneOffset.UTC));
      Assertions.assertEquals(resultSet.getDate(1), resultSet.getDate(1, null));
      Assertions.assertEquals(resultSet.getTime(1), resultSet.getTime(1, null));

      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  @Timeout(30)
  public void test_databaseMetaData() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      final DatabaseMetaData metaData = connection.getMetaData();

      Assertions.assertEquals("Apache Druid", metaData.getDatabaseProductName());
      Assertions.assertEquals("Druid JDBC Driver", metaData.getDriverName());
      Assertions.assertFalse(metaData.getDriverVersion().isEmpty());
      Assertions.assertEquals(4, metaData.getJDBCMajorVersion());
      Assertions.assertEquals(2, metaData.getJDBCMinorVersion());
      Assertions.assertTrue(metaData.isReadOnly());
      Assertions.assertTrue(metaData.allTablesAreSelectable());

      // Major and minor are parsed from the JAR implementation version, or a fallback when unpackaged.
      MatcherAssert.assertThat(metaData.getDriverMajorVersion(), Matchers.greaterThan(0));
      MatcherAssert.assertThat(metaData.getDriverMinorVersion(), Matchers.greaterThanOrEqualTo(0));

      try (ResultSet tableTypesRs = metaData.getTableTypes()) {
        final List<String> tableTypes = new ArrayList<>();
        while (tableTypesRs.next()) {
          tableTypes.add(tableTypesRs.getString("TABLE_TYPE"));
        }
        Assertions.assertTrue(tableTypes.contains("TABLE"), "Table types: " + tableTypes);
        Assertions.assertTrue(tableTypes.contains("SYSTEM_TABLE"), "Table types: " + tableTypes);
      }

      try (ResultSet schemasRs = metaData.getSchemas()) {
        final List<String> schemaNames = new ArrayList<>();
        while (schemasRs.next()) {
          schemaNames.add(schemasRs.getString("TABLE_SCHEM"));
        }
        Assertions.assertTrue(schemaNames.contains("druid"), "Schemas: " + schemaNames);
      }

      try (ResultSet tablesRs = metaData.getTables(null, null, null, null)) {
        final List<String> tableNames = new ArrayList<>();
        while (tablesRs.next()) {
          tableNames.add(tablesRs.getString("TABLE_NAME"));
          if (dataSource.equals(tablesRs.getString("TABLE_NAME"))) {
            Assertions.assertEquals("TABLE", tablesRs.getString("TABLE_TYPE"));
          }
        }
        Assertions.assertTrue(tableNames.contains(dataSource), "Tables: " + tableNames);
      }

      try (ResultSet columnsRs = metaData.getColumns(null, null, dataSource, null)) {
        final List<String> columnNames = new ArrayList<>();
        while (columnsRs.next()) {
          columnNames.add(columnsRs.getString("COLUMN_NAME"));
          Assertions.assertNotNull(columnsRs.getString("TYPE_NAME"));
          Assertions.assertTrue(columnsRs.getInt("DATA_TYPE") > 0);
        }
        Assertions.assertEquals(List.of("__time", "item", "value"), columnNames);
      }

      try (ResultSet catalogsRs = metaData.getCatalogs()) {
        final List<String> catalogNames = new ArrayList<>();
        while (catalogsRs.next()) {
          catalogNames.add(catalogsRs.getString("TABLE_CAT"));
        }
        Assertions.assertTrue(catalogNames.contains("druid"), "Catalogs: " + catalogNames);
      }
    }
  }

  protected void assertResultSet(final List<Object[]> expectedResults, final ResultSet actualResults)
      throws SQLException
  {
    final List<Object[]> actualResultsList = new ArrayList<>();
    final ResultSetMetaData metaData = actualResults.getMetaData();
    final int columnCount = metaData.getColumnCount();

    while (actualResults.next()) {
      final Object[] row = new Object[columnCount];
      for (int i = 1; i <= columnCount; i++) {
        row[i - 1] = actualResults.getObject(i);
      }
      actualResultsList.add(row);
    }

    BaseCalciteQueryTest.assertResultsEquals(
        "JDBC query results",
        expectedResults,
        actualResultsList
    );
  }

  private static String wallClock(final java.util.Date value, final ZoneId zone)
  {
    // java.sql.Date and java.sql.Time both throw from toInstant(), so go through getTime().
    return WALL_CLOCK_FORMATTER.format(Instant.ofEpochMilli(value.getTime()).atZone(zone));
  }

  protected String getJdbcUrl()
  {
    return "jdbc:druid:http://localhost:8888/druid/v2/sql/";
  }

  protected String getExpectedServerUrl()
  {
    String url = getJdbcUrl();
    url = url.substring("jdbc:druid:".length());
    final int queryStart = url.indexOf('?');
    if (queryStart >= 0) {
      url = url.substring(0, queryStart);
    }
    return url;
  }

  @Test
  @Timeout(30)
  public void test_invalidQuery_returnsJsonErrorMessage() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl());
         Statement statement = connection.createStatement()) {
      final SQLException exception = Assertions.assertThrows(
          SQLException.class,
          () -> statement.executeQuery("SELECT INVALID_FUNCTION() FROM nonexistent_table")
      );

      final String errorMessage = exception.getMessage();
      Assertions.assertTrue(errorMessage.contains("from[" + getExpectedServerUrl() + "]"), errorMessage);
      Assertions.assertTrue(errorMessage.contains("HTTP 400 error"), errorMessage);

      // The structured fields of Druid's JSON error are formatted into the message, not dumped raw.
      Assertions.assertFalse(Pattern.compile(".*\\{.*\"error\".*}.*").matcher(errorMessage).matches(), errorMessage);
    }
  }

  @Test
  @Timeout(30)
  public void test_statement_arrayAgg() throws Exception
  {
    final String query = StringUtils.format("SELECT ARRAY_AGG(item) AS items FROM \"%s\"", dataSource);

    try (Connection connection = DriverManager.getConnection(getJdbcUrl());
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(query)) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      Assertions.assertEquals(1, metaData.getColumnCount());
      Assertions.assertEquals("items", metaData.getColumnName(1));
      Assertions.assertEquals(Types.ARRAY, metaData.getColumnType(1));

      Assertions.assertTrue(resultSet.next());

      final Array array = resultSet.getArray(1);
      Assertions.assertFalse(resultSet.wasNull());
      Assertions.assertEquals(Types.VARCHAR, array.getBaseType());
      Assertions.assertEquals("VARCHAR", array.getBaseTypeName());
      Assertions.assertTrue(((Object[]) array.getArray()).length > 0);
      Assertions.assertArrayEquals((Object[]) array.getArray(), (Object[]) resultSet.getArray("items").getArray());

      Assertions.assertInstanceOf(Array.class, resultSet.getObject(1));

      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  @Timeout(30)
  public void test_preparedStatement_allTypes() throws Exception
  {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl())) {
      // A type that cannot be parameterized directly, like COMPLEX<json>, goes through a function instead.
      final String query =
          "SELECT"
          + " ? AS varchar_col,"       // 1: VARCHAR
          + " ? AS boolean_col,"       // 2: BOOLEAN
          + " ? AS integer_col,"       // 3: INTEGER
          + " ? AS bigint_col,"        // 4: BIGINT
          + " ? AS float_col,"         // 5: REAL (float)
          + " ? AS double_col,"        // 6: DOUBLE
          + " ? AS timestamp_col,"     // 7: TIMESTAMP
          + " TRY_PARSE_JSON(?) AS json_col," // 8: COMPLEX<json>
          + " ARRAY[1, 2, 3] AS array_col";   // ARRAY (literal, no parameter needed)

      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setString(1, "hello");
        statement.setBoolean(2, true);
        statement.setInt(3, 42);
        statement.setLong(4, 4000000000L);
        statement.setFloat(5, 3.14f);
        statement.setDouble(6, 2.718281828);
        statement.setTimestamp(7, Timestamp.valueOf("2025-01-15 12:30:45"));
        statement.setString(8, "{\"key\": \"value\"}");

        try (ResultSet resultSet = statement.executeQuery()) {
          final ResultSetMetaData metaData = resultSet.getMetaData();
          Assertions.assertEquals(9, metaData.getColumnCount());

          Assertions.assertTrue(resultSet.next());

          Assertions.assertEquals("hello", resultSet.getString(1));
          Assertions.assertEquals("hello", resultSet.getString("varchar_col"));
          Assertions.assertFalse(resultSet.wasNull());
          Assertions.assertEquals("CHAR", metaData.getColumnTypeName(1));

          Assertions.assertTrue(resultSet.getBoolean(2));
          Assertions.assertFalse(resultSet.wasNull());

          Assertions.assertEquals(42, resultSet.getInt(3));
          Assertions.assertFalse(resultSet.wasNull());

          Assertions.assertEquals(4000000000L, resultSet.getLong(4));
          Assertions.assertFalse(resultSet.wasNull());

          Assertions.assertEquals(3.14f, resultSet.getFloat(5), 0.01f);
          Assertions.assertFalse(resultSet.wasNull());

          Assertions.assertEquals(2.718281828, resultSet.getDouble(6), 0.000001);
          Assertions.assertFalse(resultSet.wasNull());

          final Timestamp ts = resultSet.getTimestamp(7);
          Assertions.assertNotNull(ts);
          Assertions.assertEquals(Timestamp.valueOf("2025-01-15 12:30:45"), ts);
          Assertions.assertFalse(resultSet.wasNull());

          // The parameter was bound to 12:30:45 in the JVM default time zone.
          Assertions.assertEquals(Date.valueOf("2025-01-15"), resultSet.getDate(7));
          Assertions.assertEquals(Time.valueOf("12:30:45"), resultSet.getTime(7));

          // A COMPLEX<json> value reads as parseable JSON text.
          Assertions.assertEquals("{\"key\":\"value\"}", resultSet.getString(8));
          Assertions.assertFalse(resultSet.wasNull());

          final Array array = resultSet.getArray(9);
          Assertions.assertNotNull(array);
          Assertions.assertFalse(resultSet.wasNull());
          Assertions.assertEquals(Types.BIGINT, array.getBaseType());
          Assertions.assertEquals("BIGINT", array.getBaseTypeName());
          final Object[] elements = (Object[]) array.getArray();
          Assertions.assertEquals(3, elements.length);

          for (int i = 1; i <= metaData.getColumnCount(); i++) {
            final Object obj = resultSet.getObject(i);
            if (obj != null) {
              final String expectedClassName = metaData.getColumnClassName(i);
              final Class<?> expectedClass = Class.forName(expectedClassName);
              Assertions.assertTrue(
                  expectedClass.isInstance(obj),
                  "Column " + i + " (" + metaData.getColumnName(i) + "): getColumnClassName says "
                  + expectedClassName + " but getObject returned " + obj.getClass().getName()
              );
            }
          }

          Assertions.assertEquals("hello", resultSet.getObject(1, String.class));
          Assertions.assertEquals(4000000000L, resultSet.getObject(4, Long.class));
          Assertions.assertEquals(Timestamp.valueOf("2025-01-15 12:30:45"), resultSet.getObject(7, Timestamp.class));

          Assertions.assertFalse(resultSet.next());
        }
      }

      final String nullQuery =
          "SELECT"
          + " ? AS null_varchar,"
          + " ? AS null_bigint,"
          + " ? AS null_double,"
          + " ? AS null_timestamp";

      try (PreparedStatement statement = connection.prepareStatement(nullQuery)) {
        statement.setNull(1, Types.VARCHAR);
        statement.setNull(2, Types.BIGINT);
        statement.setNull(3, Types.DOUBLE);
        statement.setNull(4, Types.TIMESTAMP);

        try (ResultSet resultSet = statement.executeQuery()) {
          Assertions.assertTrue(resultSet.next());

          resultSet.getString(1);
          Assertions.assertTrue(resultSet.wasNull());

          resultSet.getLong(2);
          Assertions.assertTrue(resultSet.wasNull());

          resultSet.getDouble(3);
          Assertions.assertTrue(resultSet.wasNull());

          resultSet.getTimestamp(4);
          Assertions.assertTrue(resultSet.wasNull());

          Assertions.assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  @Timeout(60)
  public void test_statement_cancel() throws Exception
  {
    // SLEEP must be given a non-constant argument. With a literal, Calcite constant-folds the call during planning
    // (DruidRexExecutor -> Parser.flatten), so the Broker sleeps on its planning thread, which cancellation cannot
    // interrupt. Deriving the duration from a column keeps the sleep in query execution, where cancel works.
    final String query =
        StringUtils.format("SELECT SLEEP(CAST(\"value\" AS BIGINT)) FROM \"%s\" LIMIT 3", dataSource);

    try (Connection connection = DriverManager.getConnection(getJdbcUrl());
         Statement statement = connection.createStatement();
         ExecutorService exec =
             Execs.singleThreaded(StringUtils.format("%s-test_statement_cancel-%%s", getClass().getSimpleName()))) {
      final CountDownLatch queryStarted = new CountDownLatch(1);

      try {
        final Future<?> reader = exec.submit(() -> {
          queryStarted.countDown();

          try (ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
              // Nothing to do, we're just waiting here for cancellation.
            }
          }
          return null;
        });

        Assertions.assertTrue(queryStarted.await(30, TimeUnit.SECONDS));

        // Give the Broker time to register the query, so the cancellation request finds it.
        // Same approach as SqlQueryCancelTest.
        Thread.sleep(1500);
        statement.cancel();

        final ExecutionException e = Assertions.assertThrows(ExecutionException.class, reader::get);
        MatcherAssert.assertThat(e.getCause(), Matchers.instanceOf(SQLException.class));
        MatcherAssert.assertThat(e.getCause().getMessage(), Matchers.containsString("Query cancelled"));
      }
      finally {
        exec.shutdownNow();
      }
    }
  }

  private void runIndexTask()
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    final IndexTask task =
        TaskBuilder.ofTypeIndex()
                   .dataSource(dataSource)
                   .isoTimestampColumn("time")
                   .csvInputFormatWithColumns("time", "item", "value")
                   .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                   .segmentGranularity("DAY")
                   .dimensions()
                   .withId(taskId);
    cluster.callApi().runTask(task, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
  }
}
