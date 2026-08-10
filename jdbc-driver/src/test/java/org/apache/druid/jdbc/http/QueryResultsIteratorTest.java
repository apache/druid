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

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.StringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;


public class QueryResultsIteratorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SQL_QUERY_ID = "test-sql-query-id";

  /**
   * The three header rows of a two column resultset, with the outer array left open. Truncation tests append
   * however much of the data they need.
   */
  private static final String OPEN_HEADERS =
      "[[\"name\", \"age\"], [\"STRING\", \"LONG\"], [\"VARCHAR\", \"INTEGER\"]";

  @ParameterizedTest(name = "{1}/{2}")
  @MethodSource("typeConversions")
  public void testTypeConversion(
      final String columnName,
      final String nativeType,
      final String sqlTypeName,
      final String jsonValue,
      final Class<?> expectedClass,
      final Object expectedValue
  ) throws Exception
  {
    final String responseJson = StringUtils.format(
        "[[\"%s\"], [\"%s\"], [\"%s\"], [%s]]",
        columnName,
        nativeType,
        sqlTypeName,
        jsonValue
    );

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      Assertions.assertEquals(1, iterator.getColumns().size());
      Assertions.assertEquals(columnName, iterator.getColumns().get(0).name());
      Assertions.assertEquals(JDBCType.valueOf(sqlTypeName), iterator.getColumns().get(0).type());
      Assertions.assertEquals(nativeType, iterator.getColumns().get(0).nativeType());
      Assertions.assertEquals(
          JDBCType.valueOf(sqlTypeName).getVendorTypeNumber(),
          iterator.getColumns().get(0).jdbcType()
      );

      Assertions.assertTrue(iterator.hasNext());
      final Object[] row = iterator.next();
      Assertions.assertEquals(1, row.length);
      Assertions.assertEquals(expectedClass, row[0].getClass());
      Assertions.assertEquals(expectedValue, row[0]);

      Assertions.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testNullValues() throws Exception
  {
    final String responseJson =
        "[[\"name\", \"age\", \"active\"], [\"STRING\", \"LONG\", \"LONG\"], "
        + "[\"VARCHAR\", \"INTEGER\", \"BOOLEAN\"], [null, null, null]]";

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      Assertions.assertEquals(3, iterator.getColumns().size());

      Assertions.assertTrue(iterator.hasNext());
      final Object[] row = iterator.next();
      Assertions.assertEquals(3, row.length);
      Assertions.assertNull(row[0]);
      Assertions.assertNull(row[1]);
      Assertions.assertNull(row[2]);
    }
  }

  @Test
  public void testMultipleRows() throws Exception
  {
    final String responseJson =
        "[[\"name\", \"age\"], [\"STRING\", \"LONG\"], [\"VARCHAR\", \"INTEGER\"], "
        + "[\"Alice\", 25], [\"Bob\", 30], [\"Charlie\", 35]]";

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      Assertions.assertEquals(2, iterator.getColumns().size());

      Assertions.assertTrue(iterator.hasNext());
      Object[] row = iterator.next();
      Assertions.assertEquals("Alice", row[0]);
      Assertions.assertEquals(25, row[1]);

      Assertions.assertTrue(iterator.hasNext());
      row = iterator.next();
      Assertions.assertEquals("Bob", row[0]);
      Assertions.assertEquals(30, row[1]);

      Assertions.assertTrue(iterator.hasNext());
      row = iterator.next();
      Assertions.assertEquals("Charlie", row[0]);
      Assertions.assertEquals(35, row[1]);

      Assertions.assertFalse(iterator.hasNext());
    }
  }

  /**
   * A string token or an out-of-range number cannot be returned as the Integer an INTEGER column promises.
   */
  @Test
  public void testUnreadableIntegerValueThrows() throws Exception
  {
    for (final String jsonValue : new String[]{"\"42\"", "\"not_a_number\"", "9223372036854775807"}) {
      final String responseJson =
          StringUtils.format("[[\"count\"], [\"LONG\"], [\"INTEGER\"], [%s]]", jsonValue);

      try (final QueryResultsIterator iterator = createIterator(responseJson)) {
        Assertions.assertTrue(iterator.hasNext());
        final SQLException e = Assertions.assertThrows(SQLException.class, iterator::next);
        MatcherAssert.assertThat(e.getMessage(), Matchers.containsString("of column[count] as declared type[INTEGER]"));
      }
    }
  }

  @Test
  public void testWrongNumberOfColumns() throws Exception
  {
    final String tooShort =
        "[[\"name\", \"age\"], [\"STRING\", \"LONG\"], [\"VARCHAR\", \"INTEGER\"], [\"Alice\"]]";
    try (final QueryResultsIterator iterator = createIterator(tooShort)) {
      Assertions.assertTrue(iterator.hasNext());
      final SQLException e = Assertions.assertThrows(SQLException.class, iterator::next);
      MatcherAssert.assertThat(e.getMessage(), Matchers.containsString("Data row too short"));
    }

    final String tooLong = "[[\"name\"], [\"STRING\"], [\"VARCHAR\"], [\"Alice\", \"extra_value\"]]";
    try (final QueryResultsIterator iterator = createIterator(tooLong)) {
      Assertions.assertTrue(iterator.hasNext());
      final SQLException e = Assertions.assertThrows(SQLException.class, iterator::next);
      MatcherAssert.assertThat(e.getMessage(), Matchers.containsString("Data row too long"));
    }
  }

  @Test
  public void testEmptyResponse() throws Exception
  {
    try (final QueryResultsIterator iterator = createIterator("[[],[],[]]")) {
      Assertions.assertEquals(0, iterator.getColumns().size());
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  /**
   * Elements take their Java type from the native element type, one ARRAY<> layer per level of nesting.
   */
  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("arrayValues")
  public void testArrayValues(final String nativeType, final String jsonValue, final Object expected)
      throws Exception
  {
    final String responseJson =
        StringUtils.format("[[\"arr\"], [\"%s\"], [\"ARRAY\"], [%s]]", nativeType, jsonValue);

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      Assertions.assertTrue(iterator.hasNext());
      Assertions.assertEquals(expected, iterator.next()[0]);
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("objectValues")
  public void testObjectValues(final String nativeType, final String sqlTypeName, final Object expected)
      throws Exception
  {
    final String responseJson = StringUtils.format(
        "[[\"obj\", \"after\"], [\"%s\", \"LONG\"], [\"%s\", \"BIGINT\"], [{\"a\": 1, \"b\": [\"x\"]}, 25]]",
        nativeType,
        sqlTypeName
    );

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      Assertions.assertTrue(iterator.hasNext());
      final Object[] row = iterator.next();
      Assertions.assertEquals(expected, row[0]);
      Assertions.assertEquals(25L, row[1]);
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  @ParameterizedTest(name = "cut off {0}")
  @MethodSource("truncatedResponses")
  public void testTruncatedResponseThrows(final String responseJson, final long expectedRowsRead)
  {
    assertIsTruncatedException(
        Assertions.assertThrows(SQLException.class, () -> readAll(responseJson)),
        expectedRowsRead
    );
  }

  @Test
  public void testDroppedConnectionThrows() throws Exception
  {
    try (final InputStream stream = droppedConnectionStreamOf(OPEN_HEADERS + ", [\"Alice\", 25],");
         final QueryResultsIterator iterator = new QueryResultsIteratorImpl(stream, OBJECT_MAPPER, SQL_QUERY_ID)) {
      Assertions.assertEquals("Alice", iterator.next()[0]);
      assertIsTruncatedException(Assertions.assertThrows(SQLException.class, iterator::hasNext), 1);
    }
  }

  @Test
  public void testMalformedTrailingContentIsNotReportedAsTruncation()
  {
    final SQLException e = Assertions.assertThrows(
        SQLException.class,
        () -> readAll(OPEN_HEADERS + ", [\"Alice\", 25] oops")
    );

    MatcherAssert.assertThat(e.getMessage(), Matchers.containsString("Error reading query results"));
    MatcherAssert.assertThat(e.getMessage(), Matchers.not(Matchers.containsString("Truncated response")));
  }

  @Test
  public void testInvalidJsonThrows()
  {
    Assertions.assertThrows(SQLException.class, () -> createIterator("invalid json"));
  }

  @Test
  public void testNonArrayResponseThrows()
  {
    final SQLException exception =
        Assertions.assertThrows(SQLException.class, () -> createIterator("{\"not\":\"an array\"}"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Expected array response format"));
  }

  /**
   * Header rows arrive in a fixed order: column names, then native types, then SQL types.
   */
  @Test
  public void testMissingHeaderRowsThrow()
  {
    final SQLException missingNativeTypes =
        Assertions.assertThrows(SQLException.class, () -> createIterator("[[\"only header\"]]"));
    MatcherAssert.assertThat(
        missingNativeTypes.getMessage(),
        Matchers.containsString("Response missing typesHeader row")
    );

    final SQLException missingSqlTypes =
        Assertions.assertThrows(SQLException.class, () -> createIterator("[[\"name\"], [\"LONG\"]]"));
    MatcherAssert.assertThat(
        missingSqlTypes.getMessage(),
        Matchers.containsString("Response missing sqlTypesHeader row")
    );
  }

  /**
   * A DATE reads back as the calendar day the server stored, whatever the JVM default time zone is.
   */
  @Test
  public void testDateDoesNotShiftWithDefaultTimeZone() throws Exception
  {
    // A DATE of 1990-12-25 as the server sends it under sqlTimeZone=UTC, and under sqlTimeZone=Asia/Kolkata.
    final String[] responses = {
        "[[\"d\"], [\"LONG\"], [\"DATE\"], [\"1990-12-25T00:00:00.000Z\"]]",
        "[[\"d\"], [\"LONG\"], [\"DATE\"], [\"1990-12-25T00:00:00.000+05:30\"]]"
    };

    final TimeZone original = TimeZone.getDefault();
    try {
      for (final String zoneId : new String[]{"UTC", "America/Los_Angeles", "Asia/Kolkata"}) {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

        for (final String response : responses) {
          try (final QueryResultsIterator iterator = createIterator(response)) {
            final Object value = iterator.next()[0];
            Assertions.assertEquals(
                Date.valueOf("1990-12-25"),
                value,
                StringUtils.format("zone[%s] response[%s]", zoneId, response)
            );
            Assertions.assertEquals(
                "1990-12-25",
                value.toString(),
                StringUtils.format("zone[%s] response[%s]", zoneId, response)
            );
          }
        }
      }
    }
    finally {
      TimeZone.setDefault(original);
    }
  }

  private QueryResultsIterator createIterator(final String jsonResponse) throws SQLException
  {
    return new QueryResultsIteratorImpl(streamOf(jsonResponse), OBJECT_MAPPER, SQL_QUERY_ID);
  }

  @Test
  public void testNullNativeType() throws Exception
  {
    // As of this writing, the server sends a null native type for the SQL TIME type. Verify such a response
    // is handled reasonably.
    final String responseJson = "[[\"t\"], [null], [\"TIME\"], [\"1970-01-01T15:40:30.000Z\"]]";

    try (final QueryResultsIterator iterator = createIterator(responseJson)) {
      final ColumnMetadata column = iterator.getColumns().get(0);
      Assertions.assertEquals("t", column.name());
      Assertions.assertEquals(JDBCType.TIME, column.type());
      Assertions.assertNull(column.nativeType());

      Assertions.assertTrue(iterator.hasNext());
      Assertions.assertInstanceOf(Time.class, iterator.next()[0]);
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  /**
   * Reads a response to the end, discarding the rows.
   */
  private void readAll(final String jsonResponse) throws Exception
  {
    try (final QueryResultsIterator iterator = createIterator(jsonResponse)) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
  }

  private static Stream<Arguments> typeConversions()
  {
    return Stream.of(
        Arguments.of("count", "LONG", "INTEGER", "42", Integer.class, 42),
        Arguments.of("big_id", "LONG", "BIGINT", "9223372036854775807", Long.class, 9223372036854775807L),
        Arguments.of("score", "DOUBLE", "DOUBLE", "3.14159", Double.class, 3.14159),
        Arguments.of("ratio", "FLOAT", "REAL", "2.5", Float.class, 2.5f),
        Arguments.of("rate", "DOUBLE", "FLOAT", "2.5", Double.class, 2.5),
        Arguments.of("name", "STRING", "VARCHAR", "\"alice\"", String.class, "alice"),
        Arguments.of("amount", "DOUBLE", "DECIMAL", "123.45", BigDecimal.class, new BigDecimal("123.45")),
        Arguments.of("active", "LONG", "BOOLEAN", "true", Boolean.class, Boolean.TRUE),
        // A JSON token that is not of the declared type is coerced, so a string reads as a boolean.
        Arguments.of("enabled", "LONG", "BOOLEAN", "\"FALSE\"", Boolean.class, Boolean.FALSE),
        // Any string other than "true" reads as false, following the JSON parser's boolean coercion.
        Arguments.of("maybe", "LONG", "BOOLEAN", "\"maybe\"", Boolean.class, Boolean.FALSE),
        // A numeric token in a VARCHAR column is coerced to its string representation.
        Arguments.of("text_col", "STRING", "VARCHAR", "123", String.class, "123"),
        Arguments.of(
            "created_at", "LONG", "TIMESTAMP", "\"2025-06-01 10:30:45.123\"",
            Timestamp.class, Timestamp.valueOf("2025-06-01 10:30:45.123")
        ),
        Arguments.of("birth_date", "LONG", "DATE", "\"1990-12-25\"", Date.class, Date.valueOf("1990-12-25")),
        Arguments.of("event_time", "LONG", "TIME", "\"14:30:15\"", Time.class, Time.valueOf("14:30:15")),
        // A type with no numeric or temporal reading falls back to a string.
        Arguments.of("unknown_col", "ARRAY<STRING>", "ARRAY", "\"some_value\"", String.class, "some_value")
    );
  }

  private static Stream<Arguments> arrayValues()
  {
    return Stream.of(
        Arguments.of("ARRAY<STRING>", "[\"a\", \"b\", \"c\"]", Arrays.asList("a", "b", "c")),
        Arguments.of("ARRAY<LONG>", "[1, 2, 3]", Arrays.asList(1L, 2L, 3L)),
        Arguments.of("ARRAY<DOUBLE>", "[1, 2.5]", Arrays.asList(1.0, 2.5)),
        // Elements of mixed JSON types are coerced to the declared element type.
        Arguments.of(
            "ARRAY<STRING>",
            "[1, \"two\", 3.0, true, null]",
            Arrays.asList("1", "two", "3.0", "true", null)
        ),
        Arguments.of("ARRAY<STRING>", "[]", List.of()),
        Arguments.of("ARRAY<STRING>", "null", null),
        Arguments.of("ARRAY<ARRAY<LONG>>", "[[1, 2], [3]]", Arrays.asList(Arrays.asList(1L, 2L), List.of(3L))),
        Arguments.of(
            "ARRAY<ARRAY<STRING>>",
            "[[\"a\", 2], [null]]",
            Arrays.asList(Arrays.asList("a", "2"), Collections.singletonList(null))
        ),
        // Even on a non-ARRAY column, a JSON array token reads as a List.
        Arguments.of("STRING", "[\"x\", \"y\"]", Arrays.asList("x", "y"))
    );
  }

  private static Stream<Arguments> objectValues()
  {
    final Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("a", 1);
    expected.put("b", List.of("x"));

    return Stream.of(
        Arguments.of("COMPLEX<json>", "OTHER", expected),
        Arguments.of("STRING", "VARCHAR", expected),
        Arguments.of("LONG", "BIGINT", expected),
        Arguments.of("LONG", "BOOLEAN", expected),
        Arguments.of("LONG", "TIMESTAMP", expected),
        Arguments.of("ARRAY<STRING>", "ARRAY", expected)
    );
  }

  private static Stream<Arguments> truncatedResponses()
  {
    return Stream.of(
        Arguments.of(Named.of("before anything", ""), 0L),
        Arguments.of(Named.of("inside a header row", "[[\"name\", \"age\"], [\"STR"), 0L),
        Arguments.of(Named.of("after the headers", OPEN_HEADERS), 0L),
        Arguments.of(Named.of("between rows", OPEN_HEADERS + ", [\"Alice\", 25]"), 1L),
        Arguments.of(Named.of("inside a row", OPEN_HEADERS + ", [\"Alice\", 25], [\"Bo"), 1L)
    );
  }

  private static InputStream streamOf(final String jsonResponse)
  {
    return new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Like {@link #streamOf}, but reports a dropped connection rather than EOF once the response is exhausted, the
   * way a server that goes away mid-response does.
   */
  private static InputStream droppedConnectionStreamOf(final String jsonResponse)
  {
    return new FilterInputStream(streamOf(jsonResponse))
    {
      @Override
      public int read(final byte[] buf, final int offset, final int length) throws IOException
      {
        final int bytesRead = super.read(buf, offset, length);
        if (bytesRead < 0) {
          throw new IOException("closed", new EOFException("EOF reached while reading"));
        }
        return bytesRead;
      }
    };
  }

  private static void assertIsTruncatedException(final SQLException e, final long expectedRowsRead)
  {
    MatcherAssert.assertThat(
        e.getMessage(),
        Matchers.allOf(
            Matchers.containsString(
                StringUtils.format("Truncated response after[%,d] rows", expectedRowsRead)
            ),
            Matchers.containsString("may have cut off the response"),
            Matchers.containsString("timed out"),
            Matchers.containsString(StringUtils.format("sqlQueryId[%s]", SQL_QUERY_ID))
        )
    );
  }
}
