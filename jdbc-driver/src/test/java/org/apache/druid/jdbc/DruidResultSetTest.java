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
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.TestQueryResultsIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.mockito.Mockito.mock;

public class DruidResultSetTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private DruidConnection connection;
  private DruidStatement statement;
  private List<ColumnMetadata> columns;
  private List<Object[]> rows;

  @BeforeEach
  public void setUp() throws SQLException
  {
    connection = new DruidConnection(
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/", null),
        mock(DruidHttpClient.class),
        JSON_MAPPER
    );
    statement = new DruidStatement(connection);

    columns = Arrays.asList(
        new ColumnMetadata("id", JDBCType.BIGINT),
        new ColumnMetadata("name", JDBCType.VARCHAR),
        new ColumnMetadata("score", JDBCType.DOUBLE),
        new ColumnMetadata("active", JDBCType.BOOLEAN)
    );

    rows = Arrays.asList(
        new Object[]{1L, "Alice", 95.5, true},
        new Object[]{2L, "Bob", 87.2, false},
        new Object[]{3L, null, null, null}
    );
  }

  @AfterEach
  public void tearDown() throws SQLException
  {
    // The statement is constructed directly rather than through createStatement(), so the connection does not
    // know about it and cannot close it for us.
    statement.close();
    connection.close();
  }

  private DruidResultSet resultSet(final List<ColumnMetadata> columns, final List<Object[]> rows)
  {
    return new DruidResultSet(new TestQueryResultsIterator(columns, rows), statement, JSON_MAPPER);
  }

  @Test
  public void testBasicIteration() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.isBeforeFirst());
    Assertions.assertFalse(resultSet.isFirst());
    Assertions.assertEquals(0, resultSet.getRow());

    Assertions.assertTrue(resultSet.next());
    Assertions.assertFalse(resultSet.isBeforeFirst());
    Assertions.assertTrue(resultSet.isFirst());
    Assertions.assertEquals(1, resultSet.getRow());

    Assertions.assertTrue(resultSet.next());
    Assertions.assertFalse(resultSet.isFirst());
    Assertions.assertEquals(2, resultSet.getRow());

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals(3, resultSet.getRow());

    Assertions.assertFalse(resultSet.next());
  }

  @Test
  public void testCursorStateAfterExhaustion() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    while (resultSet.next()) {
      // Advance to the end of the result set.
    }

    Assertions.assertTrue(resultSet.isAfterLast());
    Assertions.assertFalse(resultSet.isBeforeFirst());
    Assertions.assertEquals(0, resultSet.getRow());

    // The getters must throw rather than returning the last row's values.
    final SQLException e = Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
    Assertions.assertTrue(e.getMessage().contains("No current row"), "Unexpected message: " + e.getMessage());

    Assertions.assertFalse(resultSet.next());
    Assertions.assertTrue(resultSet.isAfterLast());
  }

  @Test
  public void testCursorStateEmptyResultSet() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, List.of());

    Assertions.assertFalse(resultSet.isBeforeFirst());
    Assertions.assertFalse(resultSet.next());
    Assertions.assertFalse(resultSet.isAfterLast());
    Assertions.assertEquals(0, resultSet.getRow());
    Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
  }

  @Test
  public void testDataAccessByIndexAndLabel() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals(1L, resultSet.getLong(1));
    Assertions.assertEquals("Alice", resultSet.getString(2));
    Assertions.assertEquals(95.5, resultSet.getDouble(3), 0.001);
    Assertions.assertTrue(resultSet.getBoolean(4));

    Assertions.assertEquals(1L, resultSet.getLong("id"));
    Assertions.assertEquals("Alice", resultSet.getString("name"));
    Assertions.assertEquals(95.5, resultSet.getDouble("score"), 0.001);
    Assertions.assertTrue(resultSet.getBoolean("active"));
  }

  @Test
  public void testAccessBeforeNext() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertThrows(SQLException.class, () -> resultSet.getString(1));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));

    // wasNull() returns false rather than throwing before any value has been read.
    Assertions.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testClosedResultSet() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());

    Assertions.assertThrows(SQLException.class, () -> resultSet.next());
    Assertions.assertThrows(SQLException.class, () -> resultSet.getString(1));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getMetaData());
    Assertions.assertThrows(SQLException.class, () -> resultSet.wasNull());
  }

  @Test
  public void testSetFetchDirection() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());

    for (final int direction : new int[]{ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN}) {
      final SQLException e = Assertions.assertThrows(SQLException.class, () -> resultSet.setFetchDirection(direction));
      Assertions.assertTrue(e.getMessage().contains("FETCH_FORWARD"), "Unexpected message: " + e.getMessage());
    }

    Assertions.assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());
  }

  @Test
  public void testFindColumn() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertEquals(1, resultSet.findColumn("id"));
    Assertions.assertEquals(4, resultSet.findColumn("active"));

    // Per the java.sql.ResultSet javadoc, column name lookup is case-insensitive.
    Assertions.assertEquals(1, resultSet.findColumn("ID"));
    Assertions.assertEquals(3, resultSet.findColumn("Score"));

    Assertions.assertThrows(SQLException.class, () -> resultSet.findColumn("nonexistent"));
  }

  /**
   * Per java.sql.ResultSet, the first matching column wins, and names match case-insensitively.
   */
  @Test
  public void testFindColumnReturnsFirstMatchWhenDuplicateNames() throws SQLException
  {
    final List<ColumnMetadata> dupColumns = Arrays.asList(
        new ColumnMetadata("foo", JDBCType.VARCHAR),
        new ColumnMetadata("FOO", JDBCType.VARCHAR),
        new ColumnMetadata("bar", JDBCType.VARCHAR)
    );
    final List<Object[]> dupRows = Collections.singletonList(new Object[]{"first", "second", "third"});
    final DruidResultSet resultSet = resultSet(dupColumns, dupRows);

    Assertions.assertEquals(1, resultSet.findColumn("foo"));
    Assertions.assertEquals(1, resultSet.findColumn("FOO"));
    Assertions.assertEquals(1, resultSet.findColumn("Foo"));
  }

  @Test
  public void testTemporalGetters() throws SQLException
  {
    final List<ColumnMetadata> temporalColumns = Arrays.asList(
        new ColumnMetadata("iso", JDBCType.TIMESTAMP),
        new ColumnMetadata("date_only", JDBCType.DATE),
        new ColumnMetadata("millis", JDBCType.BIGINT),
        new ColumnMetadata("null_col", JDBCType.TIMESTAMP)
    );

    // 1748781045000 == 2025-06-01T12:30:45Z, the same instant as the ISO string.
    final List<Object[]> temporalRows = Collections.singletonList(
        new Object[]{"2025-06-01T12:30:45.000Z", "2025-06-01", 1748781045000L, null}
    );

    final DruidResultSet resultSet = resultSet(temporalColumns, temporalRows);

    Assertions.assertTrue(resultSet.next());

    // The tests run with the JVM default time zone set to UTC.
    final Timestamp expectedTimestamp = Timestamp.valueOf("2025-06-01 12:30:45");
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp("iso"));
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(3));
    Assertions.assertEquals(Timestamp.valueOf("2025-06-01 00:00:00"), resultSet.getTimestamp(2));

    Assertions.assertEquals(Date.valueOf("2025-06-01"), resultSet.getDate(1));
    Assertions.assertEquals(Date.valueOf("2025-06-01"), resultSet.getDate(2));
    Assertions.assertEquals(Date.valueOf("2025-06-01"), resultSet.getDate(3));

    Assertions.assertEquals(Time.valueOf("12:30:45"), resultSet.getTime(1));
    Assertions.assertEquals(Time.valueOf("00:00:00"), resultSet.getTime(2));
    Assertions.assertEquals(Time.valueOf("12:30:45"), resultSet.getTime(3));
    Assertions.assertFalse(resultSet.wasNull());

    Assertions.assertNull(resultSet.getTimestamp(4));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getDate(4));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getTime(4));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testInvalidTimestampConversions() throws SQLException
  {
    final List<ColumnMetadata> timestampColumns = Arrays.asList(
        new ColumnMetadata("invalid_timestamp", JDBCType.VARCHAR),
        new ColumnMetadata("invalid_object", JDBCType.VARCHAR)
    );

    final List<Object[]> timestampRows = Collections.singletonList(
        new Object[]{"not-a-timestamp", new Object()}
    );

    final DruidResultSet resultSet = resultSet(timestampColumns, timestampRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));

    Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(2));
    Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(2));
  }

  @Test
  public void testGetBigDecimal() throws SQLException
  {
    final List<ColumnMetadata> decimalColumns = Arrays.asList(
        new ColumnMetadata("long_col", JDBCType.BIGINT),
        new ColumnMetadata("double_col", JDBCType.DOUBLE),
        new ColumnMetadata("string_col", JDBCType.VARCHAR),
        new ColumnMetadata("null_col", JDBCType.DOUBLE)
    );

    final List<Object[]> decimalRows = Collections.singletonList(
        new Object[]{42L, 123.456, "99.99", null}
    );

    final DruidResultSet resultSet = resultSet(decimalColumns, decimalRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals(new BigDecimal("42"), resultSet.getBigDecimal(1));
    Assertions.assertEquals(new BigDecimal("42"), resultSet.getBigDecimal("long_col"));
    Assertions.assertEquals(new BigDecimal("123.456"), resultSet.getBigDecimal(2));
    Assertions.assertFalse(resultSet.wasNull());

    // A string is not parsed, even when its contents look numeric.
    Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(3));

    Assertions.assertEquals(new BigDecimal("123.46"), resultSet.getBigDecimal(2, 2));
    Assertions.assertEquals(new BigDecimal("123"), resultSet.getBigDecimal("double_col", 0));

    Assertions.assertNull(resultSet.getBigDecimal(4));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getBigDecimal(4, 2));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetArray() throws SQLException
  {
    final List<ColumnMetadata> arrayColumns = Arrays.asList(
        new ColumnMetadata("tags", JDBCType.ARRAY, "ARRAY<STRING>"),
        new ColumnMetadata("null_arr", JDBCType.ARRAY, "ARRAY<STRING>")
    );

    final List<Object[]> arrayRows = Collections.singletonList(
        new Object[]{Arrays.asList("a", "b", "c"), null}
    );

    final DruidResultSet resultSet = resultSet(arrayColumns, arrayRows);

    Assertions.assertTrue(resultSet.next());

    final Array array = resultSet.getArray(1);
    Assertions.assertFalse(resultSet.wasNull());
    Assertions.assertEquals("VARCHAR", array.getBaseTypeName());
    Assertions.assertEquals(Types.VARCHAR, array.getBaseType());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) array.getArray());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) resultSet.getArray("tags").getArray());

    // getArray(index, count) is a one-based slice of the array.
    Assertions.assertArrayEquals(new Object[]{"b", "c"}, (Object[]) array.getArray(2, 2));

    Assertions.assertNull(resultSet.getArray(2));
    Assertions.assertTrue(resultSet.wasNull());
  }

  /**
   * An empty array has no values to inspect, so the native type string is the only source for the element type.
   */
  @ParameterizedTest(name = "{0} -> {1}")
  @CsvSource({
      "ARRAY<LONG>,          BIGINT",
      "ARRAY<STRING>,        VARCHAR",
      "ARRAY<DOUBLE>,        DOUBLE",
      "ARRAY<FLOAT>,         FLOAT",
      "ARRAY<ARRAY<LONG>>,   ARRAY",
      "ARRAY<COMPLEX<json>>, OTHER"
  })
  public void testGetArrayElementTypeFromNativeType(final String nativeType, final String expectedBaseTypeName)
      throws SQLException
  {
    final List<ColumnMetadata> arrayColumns = List.of(
        new ColumnMetadata("arr", JDBCType.ARRAY, nativeType)
    );
    final DruidResultSet resultSet =
        resultSet(arrayColumns, Collections.singletonList(new Object[]{Collections.emptyList()}));

    Assertions.assertTrue(resultSet.next());

    final Array array = resultSet.getArray(1);
    Assertions.assertFalse(resultSet.wasNull());
    Assertions.assertEquals(JDBCType.valueOf(expectedBaseTypeName).getVendorTypeNumber(), array.getBaseType());
    Assertions.assertEquals(expectedBaseTypeName, array.getBaseTypeName());
    Assertions.assertEquals(0, ((Object[]) array.getArray()).length);
  }

  @Test
  public void testGetterOverloadsWithCalendar() throws SQLException
  {
    final List<ColumnMetadata> timestampColumns = List.of(
        new ColumnMetadata("timestamp_col", JDBCType.TIMESTAMP)
    );
    final DruidResultSet resultSet = resultSet(
        timestampColumns,
        Collections.singletonList(new Object[]{"2025-06-01T12:30:45.000Z"})
    );

    Assertions.assertTrue(resultSet.next());

    // 2025-06-01T12:30:45Z reads as 05:30:45 on 2025-06-01 in Los Angeles, which is on PDT (UTC-7) then.
    final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"), Locale.ENGLISH);

    final Timestamp expectedTimestamp = Timestamp.from(Instant.parse("2025-06-01T12:30:45Z"));
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1, cal));
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp("timestamp_col", cal));

    // Midnight of 2025-06-01 in Los Angeles is 07:00Z.
    final Date expectedDate = new Date(Instant.parse("2025-06-01T07:00:00Z").toEpochMilli());
    Assertions.assertEquals(expectedDate, resultSet.getDate(1, cal));
    Assertions.assertEquals(expectedDate, resultSet.getDate("timestamp_col", cal));

    // 05:30:45 on the epoch day in Los Angeles is 13:30:45Z, since the zone was on PST (UTC-8) in 1970.
    final Time expectedTime = new Time(Instant.parse("1970-01-01T13:30:45Z").toEpochMilli());
    Assertions.assertEquals(expectedTime, resultSet.getTime(1, cal));
    Assertions.assertEquals(expectedTime, resultSet.getTime("timestamp_col", cal));

    // A null calendar means the JVM default time zone, which these tests run with set to UTC.
    Assertions.assertEquals(expectedTimestamp, resultSet.getTimestamp(1, null));
    Assertions.assertEquals(Date.valueOf("2025-06-01"), resultSet.getDate(1, null));
    Assertions.assertEquals(resultSet.getDate(1), resultSet.getDate(1, null));
    Assertions.assertEquals(Time.valueOf("12:30:45"), resultSet.getTime(1, null));
    Assertions.assertEquals(resultSet.getTime(1), resultSet.getTime(1, null));

    Assertions.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testGetArrayOnNonArrayColumn() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertThrows(SQLException.class, () -> resultSet.getArray(1));
  }

  /**
   * A multi-value string dimension arrives as a list on a column whose declared type is VARCHAR.
   */
  @Test
  public void testGetMultiValueStringDimension() throws SQLException
  {
    final List<ColumnMetadata> mvColumns = List.of(
        new ColumnMetadata("tags", JDBCType.VARCHAR, "STRING")
    );
    final List<Object[]> mvRows = Collections.singletonList(
        new Object[]{Arrays.asList("a", "b")}
    );

    final DruidResultSet resultSet = resultSet(mvColumns, mvRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getString(1));
    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getString("tags"));
    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getObject(1));
    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getObject("tags"));
    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getObject(1, String.class));
    Assertions.assertEquals("[\"a\",\"b\"]", resultSet.getObject(1, Object.class));

    final Array array = resultSet.getArray(1);
    Assertions.assertEquals(Types.VARCHAR, array.getBaseType());
    Assertions.assertEquals("VARCHAR", array.getBaseTypeName());
    Assertions.assertArrayEquals(new Object[]{"a", "b"}, (Object[]) array.getArray());
  }

  @Test
  public void testGetStringAndObjectOnArrayColumn() throws SQLException
  {
    final List<ColumnMetadata> arrayColumns = List.of(
        new ColumnMetadata("nums", JDBCType.ARRAY, "ARRAY<LONG>")
    );
    final List<Object[]> arrayRows = Collections.singletonList(
        new Object[]{Arrays.asList(1L, null, 3L)}
    );

    final DruidResultSet resultSet = resultSet(arrayColumns, arrayRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals("[1,null,3]", resultSet.getString(1));

    final Array array = Assertions.assertInstanceOf(Array.class, resultSet.getObject(1));
    Assertions.assertArrayEquals(new Object[]{1L, null, 3L}, (Object[]) array.getArray());
    Assertions.assertInstanceOf(Array.class, resultSet.getObject("nums"));
    Assertions.assertInstanceOf(Array.class, resultSet.getObject(1, Array.class));
    Assertions.assertInstanceOf(Array.class, resultSet.getObject(1, Object.class));

    // A String is still requestable, and still comes from the underlying list rather than the Array.
    Assertions.assertEquals("[1,null,3]", resultSet.getObject(1, String.class));
  }

  @Test
  public void testGetStringAndObjectOnJsonColumn() throws SQLException
  {
    final List<ColumnMetadata> jsonColumns = List.of(
        new ColumnMetadata("stringified", JDBCType.OTHER, "COMPLEX<json>"),
        new ColumnMetadata("structured", JDBCType.OTHER, "COMPLEX<json>")
    );
    final Map<String, Object> structured = new LinkedHashMap<>();
    structured.put("a", 1);
    structured.put("b", List.of("x", "y"));
    final List<Object[]> jsonRows = Collections.singletonList(new Object[]{"{\"a\":1}", structured});

    final DruidResultSet resultSet = resultSet(jsonColumns, jsonRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals("{\"a\":1}", resultSet.getString(1));
    Assertions.assertEquals("{\"a\":1,\"b\":[\"x\",\"y\"]}", resultSet.getString(2));
    Assertions.assertEquals("{\"a\":1,\"b\":[\"x\",\"y\"]}", resultSet.getString("structured"));
    Assertions.assertEquals("{\"a\":1,\"b\":[\"x\",\"y\"]}", resultSet.getObject(2, String.class));

    // getObject returns the value as it was read.
    Assertions.assertEquals(structured, resultSet.getObject(2));
  }

  @Test
  public void testGetObjectWithType() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.next());

    // No conversion needed: the value is already an instance of the requested class.
    Assertions.assertEquals(1L, resultSet.getObject(1));
    Assertions.assertEquals(1L, resultSet.getObject(1, Long.class));
    Assertions.assertEquals(1L, resultSet.getObject(1, Object.class));
    Assertions.assertEquals("Alice", resultSet.getObject(2, String.class));
    Assertions.assertEquals(95.5, resultSet.getObject(3, Double.class));
    Assertions.assertEquals(true, resultSet.getObject(4, Boolean.class));
    Assertions.assertFalse(resultSet.wasNull());

    // Numeric narrowing and widening, matching the corresponding typed getters.
    Assertions.assertEquals(1, resultSet.getObject(1, Integer.class));
    Assertions.assertEquals((short) 1, resultSet.getObject(1, Short.class));
    Assertions.assertEquals((byte) 1, resultSet.getObject(1, Byte.class));
    Assertions.assertEquals(1.0f, resultSet.getObject(1, Float.class));
    Assertions.assertEquals(1.0, resultSet.getObject(1, Double.class));
    Assertions.assertEquals(new BigDecimal("1"), resultSet.getObject(1, BigDecimal.class));
    Assertions.assertEquals("1", resultSet.getObject(1, String.class));
    Assertions.assertEquals(true, resultSet.getObject(1, Boolean.class));

    // A class the driver cannot produce is an error rather than a null.
    final SQLException e = Assertions.assertThrows(SQLException.class, () -> resultSet.getObject(1, Array.class));
    Assertions.assertTrue(e.getMessage().contains("Array"), "Unexpected message: " + e.getMessage());

    // Nulls come back as null for every requested class, and set wasNull().
    Assertions.assertTrue(resultSet.next());
    Assertions.assertTrue(resultSet.next());
    Assertions.assertNull(resultSet.getObject(2));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getObject(2, String.class));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getObject(3, Double.class));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getObject(4, Boolean.class));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testGetObjectWithTemporalType() throws SQLException
  {
    final List<ColumnMetadata> timestampColumns = List.of(
        new ColumnMetadata("timestamp_col", JDBCType.TIMESTAMP)
    );
    final DruidResultSet resultSet = resultSet(
        timestampColumns,
        Collections.singletonList(new Object[]{"2025-06-01T12:30:45.000Z"})
    );

    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals(resultSet.getTimestamp(1), resultSet.getObject(1, Timestamp.class));
    Assertions.assertEquals(resultSet.getDate(1), resultSet.getObject(1, Date.class));
    Assertions.assertEquals(resultSet.getTime(1), resultSet.getObject(1, Time.class));
  }

  /**
   * Druid's SQL JSON response encodes VARBINARY and COMPLEX values as base64 strings.
   */
  @Test
  public void testGetBytes() throws SQLException
  {
    final byte[] bytes = {1, 2, 3};
    final List<ColumnMetadata> binaryColumns = Arrays.asList(
        new ColumnMetadata("base64_col", JDBCType.VARCHAR),
        new ColumnMetadata("bytes_col", JDBCType.VARCHAR),
        new ColumnMetadata("not_base64_col", JDBCType.VARCHAR),
        new ColumnMetadata("null_col", JDBCType.VARCHAR)
    );
    final List<Object[]> binaryRows = Collections.singletonList(
        new Object[]{Base64.getEncoder().encodeToString(bytes), bytes, "not base64!", null}
    );

    final DruidResultSet resultSet = resultSet(binaryColumns, binaryRows);

    Assertions.assertTrue(resultSet.next());

    Assertions.assertArrayEquals(bytes, resultSet.getBytes(1));
    Assertions.assertFalse(resultSet.wasNull());
    Assertions.assertArrayEquals(bytes, resultSet.getBytes("base64_col"));
    Assertions.assertArrayEquals(bytes, resultSet.getObject(1, byte[].class));

    // A byte[] straight from the row is returned as-is.
    Assertions.assertArrayEquals(bytes, resultSet.getBytes(2));

    // A string that is not valid base64 is an error.
    Assertions.assertThrows(SQLException.class, () -> resultSet.getBytes(3));

    Assertions.assertNull(resultSet.getBytes(4));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testCreateEmpty() throws SQLException
  {
    final ResultSet resultSet = DruidResultSet.createEmpty(columns);

    Assertions.assertEquals(4, resultSet.getMetaData().getColumnCount());
    Assertions.assertEquals("id", resultSet.getMetaData().getColumnName(1));

    // There is no query, so no Statement either.
    Assertions.assertNull(resultSet.getStatement());

    Assertions.assertFalse(resultSet.isBeforeFirst());
    Assertions.assertFalse(resultSet.next());
    Assertions.assertFalse(resultSet.isAfterLast());
    Assertions.assertEquals(0, resultSet.getRow());
    Assertions.assertThrows(SQLException.class, () -> resultSet.getString(1));

    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
  }

  @Test
  @SuppressWarnings("UseOfIndexZeroInJDBCResultSet")
  public void testColumnLookupErrorMessages() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.next());

    final SQLException indexTooHigh = Assertions.assertThrows(SQLException.class, () -> resultSet.getString(5));
    Assertions.assertEquals("Invalid column index[5]. Valid range[1-4]", indexTooHigh.getMessage());

    final SQLException indexZero = Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0));
    Assertions.assertEquals("Invalid column index[0]. Valid range[1-4]", indexZero.getMessage());

    final SQLException noSuchColumn =
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString("nonexistent"));
    Assertions.assertEquals("Column not found: nonexistent", noSuchColumn.getMessage());
  }

  @Test
  public void testWrapperInterface() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(columns, rows);

    Assertions.assertTrue(resultSet.isWrapperFor(DruidResultSet.class));
    Assertions.assertTrue(resultSet.isWrapperFor(ResultSet.class));
    Assertions.assertFalse(resultSet.isWrapperFor(String.class));

    Assertions.assertSame(resultSet, resultSet.unwrap(DruidResultSet.class));
    Assertions.assertSame(resultSet, resultSet.unwrap(ResultSet.class));

    final SQLException e = Assertions.assertThrows(SQLException.class, () -> resultSet.unwrap(String.class));
    Assertions.assertEquals("Cannot unwrap to class[java.lang.String]", e.getMessage());
  }

  /**
   * Closing the results iterator is what releases the HTTP response.
   */
  @Test
  public void testCloseClosesResultsIterator() throws SQLException
  {
    final TestQueryResultsIterator resultsIterator = new TestQueryResultsIterator(columns, rows);
    final DruidResultSet resultSet = new DruidResultSet(resultsIterator, statement, JSON_MAPPER);

    Assertions.assertFalse(resultsIterator.isClosed());

    resultSet.close();
    Assertions.assertTrue(resultsIterator.isClosed());

    // Closing twice is harmless.
    resultSet.close();
    Assertions.assertTrue(resultSet.isClosed());
  }
}
