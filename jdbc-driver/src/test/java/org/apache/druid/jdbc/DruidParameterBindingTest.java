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
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.SqlParameter;
import org.apache.druid.jdbc.http.SqlRequest;
import org.apache.druid.jdbc.http.TestQueryResultsIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DruidParameterBindingTest
{
  private static final Calendar LOS_ANGELES =
      Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"), Locale.ENGLISH);

  // 2009-02-13T23:31:30Z, which is 15:31:30 the same day in Los Angeles (PST, UTC-8).
  private static final long INSTANT_MILLIS = 1234567890000L;

  @Mock
  private DruidHttpClient mockHttpClient;

  private DruidConnection connection;
  private DruidPreparedStatement preparedStatement;

  @BeforeEach
  public void setUp() throws SQLException
  {
    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/", null),
        mockHttpClient,
        new ObjectMapper()
    );

    // These tests assert on the request that was sent, not on the results, but the query must still come back
    // with a response, since DruidHttpClient#runQuery throws rather than returning null.
    lenient().when(mockHttpClient.runQuery(any())).thenReturn(TestQueryResultsIterator.empty(List.of()));

    preparedStatement = new DruidPreparedStatement(connection, "SELECT * FROM test WHERE col = ?");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bindings")
  public void testBinding(
      final Binder binder,
      final String expectedType,
      final Object expectedValue
  ) throws SQLException
  {
    binder.bind(preparedStatement);
    preparedStatement.executeQuery();

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(requestCaptor.capture());

    // The value must be bound, never spliced into the SQL text.
    Assertions.assertEquals("SELECT * FROM test WHERE col = ?", requestCaptor.getValue().query());

    final List<SqlParameter> parameters = requestCaptor.getValue().parameters();
    Assertions.assertEquals(1, parameters.size());
    Assertions.assertEquals(expectedType, parameters.get(0).type());
    Assertions.assertEquals(expectedValue, parameters.get(0).value());
  }

  private static Stream<Arguments> bindings()
  {
    final URL url = exampleUrl();
    final BigDecimal decimal = new BigDecimal("123.456");

    return Stream.of(
        // Scalar setters.
        binding("setString", ps -> ps.setString(1, "test_value"), "VARCHAR", "test_value"),
        binding("setString(null)", ps -> ps.setString(1, null), "VARCHAR", null),
        binding("setString(sql)", ps -> ps.setString(1, "x'; DROP TABLE t; --"), "VARCHAR", "x'; DROP TABLE t; --"),
        binding("setNString", ps -> ps.setNString(1, "unicode_string"), "VARCHAR", "unicode_string"),
        binding("setBoolean", ps -> ps.setBoolean(1, true), "BOOLEAN", true),
        binding("setByte", ps -> ps.setByte(1, (byte) 127), "TINYINT", (byte) 127),
        binding("setShort", ps -> ps.setShort(1, (short) 32767), "SMALLINT", (short) 32767),
        binding("setInt", ps -> ps.setInt(1, 42), "INTEGER", 42),
        binding("setLong", ps -> ps.setLong(1, 123456789L), "BIGINT", 123456789L),
        binding("setFloat", ps -> ps.setFloat(1, 3.14f), "REAL", 3.14f),
        binding("setDouble", ps -> ps.setDouble(1, 3.14159), "DOUBLE", 3.14159),
        binding("setBigDecimal", ps -> ps.setBigDecimal(1, decimal), "DECIMAL", decimal),
        binding("setURL", ps -> ps.setURL(1, url), "VARCHAR", "http://example.com"),
        binding("setURL(null)", ps -> ps.setURL(1, null), "VARCHAR", null),
        binding("setNull", ps -> ps.setNull(1, Types.INTEGER), "INTEGER", null),
        binding("setNull(typeName)", ps -> ps.setNull(1, Types.VARCHAR, "VARCHAR"), "VARCHAR", null),

        // Temporal setters. Timestamps go on the wire as UTC ISO-8601; dates and times are normalized in the
        // time zone of the Calendar argument, or the JVM default when there is none.
        binding("setTimestamp", ps -> ps.setTimestamp(1, new Timestamp(0L)), "TIMESTAMP", "1970-01-01T00:00:00.000Z"),
        binding("setTimestamp(null)", ps -> ps.setTimestamp(1, null), "TIMESTAMP", null),
        binding("setDate", ps -> ps.setDate(1, Date.valueOf("2025-03-15")), "DATE", "2025-03-15"),
        binding("setDate(null)", ps -> ps.setDate(1, null), "DATE", null),
        binding(
            "setTimestamp(cal=null)",
            ps -> ps.setTimestamp(1, new Timestamp(INSTANT_MILLIS), null),
            "TIMESTAMP",
            "2009-02-13T23:31:30.000Z"
        ),
        binding(
            "setTimestamp(cal=LA)",
            ps -> ps.setTimestamp(1, new Timestamp(INSTANT_MILLIS), LOS_ANGELES),
            "TIMESTAMP",
            "2009-02-13T23:31:30.000Z"
        ),
        binding("setDate(cal=null)", ps -> ps.setDate(1, Date.valueOf("2025-03-15"), null), "DATE", "2025-03-15"),
        binding("setDate(cal=LA)", ps -> ps.setDate(1, new Date(INSTANT_MILLIS), LOS_ANGELES), "DATE", "2009-02-13"),

        // setObject infers the wire type from the value's class.
        binding("setObject(null)", ps -> ps.setObject(1, null), "VARCHAR", null),
        binding("setObject(String)", ps -> ps.setObject(1, "test_string"), "VARCHAR", "test_string"),
        binding("setObject(Integer)", ps -> ps.setObject(1, 100), "INTEGER", 100),
        binding("setObject(Short)", ps -> ps.setObject(1, (short) 42), "SMALLINT", (short) 42),
        binding("setObject(Byte)", ps -> ps.setObject(1, (byte) 7), "TINYINT", (byte) 7),
        binding(
            "setObject(UUID)",
            ps -> ps.setObject(1, UUID.fromString("00000000-0000-0000-0000-000000000000")),
            "VARCHAR",
            "00000000-0000-0000-0000-000000000000"
        ),
        binding("setObject(LocalDate)", ps -> ps.setObject(1, LocalDate.of(2025, 3, 15)), "DATE", "2025-03-15"),
        // A SQL TIMESTAMP (without time zone) carries no zone, so LocalDateTime keeps no zone designator.
        binding(
            "setObject(LocalDateTime)",
            ps -> ps.setObject(1, LocalDateTime.of(2025, 3, 15, 12, 34, 56)),
            "TIMESTAMP",
            "2025-03-15T12:34:56"
        ),
        binding(
            "setObject(OffsetDateTime)",
            ps -> ps.setObject(1, OffsetDateTime.of(2025, 3, 15, 12, 34, 56, 0, ZoneOffset.ofHours(-5))),
            "TIMESTAMP",
            "2025-03-15T17:34:56.000Z"
        ),
        // EDT (UTC-4) applies in New York on this date.
        binding(
            "setObject(ZonedDateTime)",
            ps -> ps.setObject(1, ZonedDateTime.of(2025, 3, 15, 12, 34, 56, 0, ZoneId.of("America/New_York"))),
            "TIMESTAMP",
            "2025-03-15T16:34:56.000Z"
        ),
        binding("setObject(Instant)", ps -> ps.setObject(1, Instant.ofEpochMilli(0L)), "TIMESTAMP",
                "1970-01-01T00:00:00.000Z"),

        binding("setObject(null, INTEGER)", ps -> ps.setObject(1, null, Types.INTEGER), "INTEGER", null),
        binding("setObject(String, INTEGER)", ps -> ps.setObject(1, "123", Types.INTEGER), "INTEGER", 123),
        binding("setObject(String, BIGINT)", ps -> ps.setObject(1, "123", Types.BIGINT), "BIGINT", 123L),
        binding("setObject(String, DOUBLE)", ps -> ps.setObject(1, "1.5", Types.DOUBLE), "DOUBLE", 1.5d),
        binding("setObject(String, REAL)", ps -> ps.setObject(1, "1.5", Types.REAL), "REAL", 1.5f),
        binding("setObject(String, BOOLEAN)", ps -> ps.setObject(1, "true", Types.BOOLEAN), "BOOLEAN", true),
        binding("setObject(Integer, VARCHAR)", ps -> ps.setObject(1, 42, Types.VARCHAR), "VARCHAR", "42"),
        binding("setObject(Double, BIGINT)", ps -> ps.setObject(1, 3.9d, Types.BIGINT), "BIGINT", 3L),
        binding(
            "setObject(String, TIMESTAMP)",
            ps -> ps.setObject(1, "2020-01-01 00:00:00", Types.TIMESTAMP),
            "TIMESTAMP",
            "2020-01-01T00:00:00.000Z"
        ),
        binding(
            "setObject(String, DATE)",
            ps -> ps.setObject(1, "2025-03-15", Types.DATE),
            "DATE",
            "2025-03-15"
        ),
        binding(
            "setObject(targetType, scale)",
            ps -> ps.setObject(1, new BigDecimal("123.45"), Types.DECIMAL, 2),
            "DECIMAL",
            new BigDecimal("123.45")
        ),
        binding(
            "setObject(Instant, targetType)",
            ps -> ps.setObject(1, Instant.parse("2020-01-01T00:00:00Z"), Types.TIMESTAMP),
            "TIMESTAMP",
            "2020-01-01T00:00:00.000Z"
        ),
        binding(
            "setObject(LocalTime, VARCHAR)",
            ps -> ps.setObject(1, LocalTime.of(12, 34, 56), Types.VARCHAR),
            "VARCHAR",
            "12:34:56"
        )
    );
  }

  private static Arguments binding(
      final String name,
      final Binder binder,
      final String expectedType,
      final Object expectedValue
  )
  {
    return Arguments.of(Named.of(name, binder), expectedType, expectedValue);
  }

  private static URL exampleUrl()
  {
    try {
      return URI.create("http://example.com").toURL();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A date wire value must not shift with the JVM default time zone; a timestamp is always UTC.
   */
  @Test
  public void testNonUtcDefaultTimezone() throws SQLException
  {
    final TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

      preparedStatement = new DruidPreparedStatement(
          connection,
          "SELECT * FROM test WHERE d = ? AND ts = ?"
      );
      preparedStatement.setDate(1, Date.valueOf("2025-03-15"));
      preparedStatement.setTimestamp(2, new Timestamp(0L));

      preparedStatement.executeQuery();

      final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
      verify(mockHttpClient).runQuery(requestCaptor.capture());

      final List<SqlParameter> parameters = requestCaptor.getValue().parameters();
      Assertions.assertEquals(2, parameters.size());
      Assertions.assertEquals("2025-03-15", parameters.get(0).value());
      Assertions.assertEquals("1970-01-01T00:00:00.000Z", parameters.get(1).value());
    }
    finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  public void testMultipleParametersKeepBindingOrder() throws SQLException
  {
    preparedStatement = new DruidPreparedStatement(
        connection,
        "SELECT * FROM test WHERE col1 = ? AND col2 = ? AND col3 = ?"
    );

    preparedStatement.setBoolean(3, false);
    preparedStatement.setString(1, "first");
    preparedStatement.setInt(2, 42);

    preparedStatement.executeQuery();

    final ArgumentCaptor<SqlRequest> requestCaptor = ArgumentCaptor.forClass(SqlRequest.class);
    verify(mockHttpClient).runQuery(requestCaptor.capture());

    final List<SqlParameter> parameters = requestCaptor.getValue().parameters();
    Assertions.assertEquals(3, parameters.size());
    Assertions.assertEquals(new SqlParameter("VARCHAR", "first"), parameters.get(0));
    Assertions.assertEquals(new SqlParameter("INTEGER", 42), parameters.get(1));
    Assertions.assertEquals(new SqlParameter("BOOLEAN", false), parameters.get(2));
  }

  @Test
  public void testSetObjectWithUnknownTypeThrows()
  {
    class CustomObject
    {
    }

    final CustomObject value = new CustomObject();
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> preparedStatement.setObject(1, value)
    );
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(value.getClass().getName()));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("setObject"));
  }

  @Test
  public void testSetObjectNumberAsBooleanThrows()
  {
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> preparedStatement.setObject(1, 1, Types.BOOLEAN)
    );
    Assertions.assertEquals(DruidSQLState.InvalidParameterType.getSqlState(), exception.getSQLState());
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(Integer.class.getName()));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("BOOLEAN"));
  }

  @Test
  public void testSetObjectWithUnknownSqlTypeThrows()
  {
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> preparedStatement.setObject(1, "test", 9999)
    );
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Unsupported SQL type"));
  }


  @FunctionalInterface
  private interface Binder
  {
    void bind(DruidPreparedStatement preparedStatement) throws SQLException;
  }
}
