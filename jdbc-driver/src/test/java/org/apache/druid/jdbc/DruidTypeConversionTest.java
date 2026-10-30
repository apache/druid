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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A single VARCHAR column throughout, so that the row values rather than the column type drive the behavior.
 */
public class DruidTypeConversionTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final List<ColumnMetadata> COLUMNS =
      Collections.singletonList(new ColumnMetadata("value", JDBCType.VARCHAR));

  private static DruidResultSet resultSet(final Object... values)
  {
    final List<Object[]> rows = Arrays.stream(values).map(v -> new Object[]{v}).toList();
    return new DruidResultSet(new TestQueryResultsIterator(COLUMNS, rows), null, JSON_MAPPER);
  }

  /**
   * The numeric getters convert freely among the numeric types, narrowing and widening as needed.
   */
  @Test
  public void testNumericConversions() throws SQLException
  {
    assertNumericConversions(123, 123, 123L, 123.0, 123.0f, (short) 123, (byte) 123);
    assertNumericConversions(123L, 123, 123L, 123.0, 123.0f, (short) 123, (byte) 123);
    assertNumericConversions(123.45, 123, 123L, 123.45, 123.45f, (short) 123, (byte) 123);
    assertNumericConversions(123.45f, 123, 123L, 123.45000457763672, 123.45f, (short) 123, (byte) 123);
  }

  private void assertNumericConversions(
      final Object input,
      final int expectedInt,
      final long expectedLong,
      final double expectedDouble,
      final float expectedFloat,
      final short expectedShort,
      final byte expectedByte
  ) throws SQLException
  {
    final DruidResultSet resultSet = resultSet(input);
    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals(expectedInt, resultSet.getInt(1));
    Assertions.assertEquals(expectedLong, resultSet.getLong(1));
    Assertions.assertEquals(expectedDouble, resultSet.getDouble(1), 0.001);
    Assertions.assertEquals(expectedFloat, resultSet.getFloat(1), 0.001f);
    Assertions.assertEquals(expectedShort, resultSet.getShort(1));
    Assertions.assertEquals(expectedByte, resultSet.getByte(1));
    Assertions.assertFalse(resultSet.wasNull());
  }

  @Test
  public void testNarrowingWrapsAround() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(Long.MAX_VALUE, Integer.MAX_VALUE + 1L, 128);

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals(-1, resultSet.getInt(1));
    Assertions.assertEquals(Long.MAX_VALUE, resultSet.getLong(1));

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals(Integer.MIN_VALUE, resultSet.getInt(1));
    Assertions.assertEquals(Integer.MAX_VALUE + 1L, resultSet.getLong(1));

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals((byte) -128, resultSet.getByte(1));
  }

  @Test
  public void testNonNumericValuesRejected() throws SQLException
  {
    // Strings are never parsed, even when their contents look numeric.
    for (final Object value : new Object[]{"123", "456.78", "-999", "not_a_number", new Object()}) {
      final DruidResultSet resultSet = resultSet(value);
      Assertions.assertTrue(resultSet.next());

      Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
      Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(1));
    }
  }

  /**
   * "true" and "1" (in any case) and any nonzero number are true; anything else readable is false.
   */
  @Test
  public void testBooleanConversions() throws SQLException
  {
    final DruidResultSet resultSet =
        resultSet(true, false, "true", "TRUE", "false", "1", "0", "maybe", 1, 0, 42);
    final boolean[] expected = {true, false, true, true, false, true, false, false, true, false, true};

    for (final boolean value : expected) {
      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals(value, resultSet.getBoolean(1));
    }

    Assertions.assertFalse(resultSet.next());
  }

  @Test
  public void testUnsupportedTypeRejectedByGetBoolean() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(new Object());
    Assertions.assertTrue(resultSet.next());
    Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
  }

  @Test
  public void testNullConversions() throws SQLException
  {
    final DruidResultSet resultSet = resultSet((Object) null);
    Assertions.assertTrue(resultSet.next());

    Assertions.assertEquals(0, resultSet.getInt(1));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertEquals(0L, resultSet.getLong(1));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertEquals(0.0, resultSet.getDouble(1), 0.001);
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertEquals(0.0f, resultSet.getFloat(1), 0.001f);
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertEquals((short) 0, resultSet.getShort(1));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertEquals((byte) 0, resultSet.getByte(1));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertFalse(resultSet.getBoolean(1));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertNull(resultSet.getString(1));
    Assertions.assertTrue(resultSet.wasNull());
  }

  @Test
  public void testStringConversions() throws SQLException
  {
    final DruidResultSet resultSet = resultSet(123, 123.45, true, null);

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals("123", resultSet.getString(1));

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals("123.45", resultSet.getString(1));

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals("true", resultSet.getString(1));
    Assertions.assertFalse(resultSet.wasNull());

    Assertions.assertTrue(resultSet.next());
    Assertions.assertNull(resultSet.getString(1));
    Assertions.assertTrue(resultSet.wasNull());
  }
}
