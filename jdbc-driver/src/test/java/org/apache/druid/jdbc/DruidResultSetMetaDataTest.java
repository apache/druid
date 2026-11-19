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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;


public class DruidResultSetMetaDataTest
{
  @ParameterizedTest(name = "{0}")
  @CsvSource({
      "BIGINT,    java.lang.Long,       20,  true,  false",
      "INTEGER,   java.lang.Integer,    11,  true,  false",
      "SMALLINT,  java.lang.Integer,    6,   true,  false",
      "TINYINT,   java.lang.Integer,    4,   true,  false",
      "DOUBLE,    java.lang.Double,     25,  true,  false",
      "FLOAT,     java.lang.Double,     15,  true,  false",
      "REAL,      java.lang.Float,      15,  true,  false",
      "BOOLEAN,   java.lang.Boolean,    512, false, false",
      "VARCHAR,   java.lang.String,     512, false, true",
      "CHAR,      java.lang.String,     512, false, true",
      "TIMESTAMP, java.sql.Timestamp,   29,  false, false",
      "DATE,      java.sql.Date,        10,  false, false"
  })
  public void testPrimitiveColumnTypeProperties(
      final JDBCType type,
      final String expectedClassName,
      final int expectedDisplaySize,
      final boolean expectedSigned,
      final boolean expectedCaseSensitive
  ) throws SQLException
  {
    final DruidResultSetMetaData metaData = new DruidResultSetMetaData(List.of(new ColumnMetadata("c", type)));

    Assertions.assertEquals(1, metaData.getColumnCount());
    Assertions.assertEquals("c", metaData.getColumnName(1));
    // Druid results carry no column alias separate from the column name.
    Assertions.assertEquals("c", metaData.getColumnLabel(1));
    Assertions.assertEquals(type.getVendorTypeNumber(), metaData.getColumnType(1));
    Assertions.assertEquals(type.getName(), metaData.getColumnTypeName(1));
    Assertions.assertEquals(expectedClassName, metaData.getColumnClassName(1));
    Assertions.assertEquals(expectedDisplaySize, metaData.getColumnDisplaySize(1));
    Assertions.assertEquals(expectedSigned, metaData.isSigned(1));
    Assertions.assertEquals(expectedCaseSensitive, metaData.isCaseSensitive(1));
  }

  @Test
  public void testArrayColumnClassName() throws SQLException
  {
    final DruidResultSetMetaData metaData =
        new DruidResultSetMetaData(List.of(new ColumnMetadata("c", JDBCType.ARRAY, "ARRAY<LONG>")));

    Assertions.assertEquals(Array.class.getName(), metaData.getColumnClassName(1));
  }

  @Test
  public void testInvalidColumnIndex()
  {
    final DruidResultSetMetaData metaData =
        new DruidResultSetMetaData(List.of(new ColumnMetadata("c", JDBCType.VARCHAR)));

    Assertions.assertThrows(SQLException.class, () -> metaData.getColumnName(0));
    Assertions.assertThrows(SQLException.class, () -> metaData.getColumnName(2));
    Assertions.assertThrows(SQLException.class, () -> metaData.getColumnType(-1));
    Assertions.assertThrows(SQLException.class, () -> metaData.getColumnTypeName(100));
  }

  @Test
  public void testWrapperMethods() throws SQLException
  {
    final DruidResultSetMetaData metaData =
        new DruidResultSetMetaData(List.of(new ColumnMetadata("c", JDBCType.VARCHAR)));

    Assertions.assertTrue(metaData.isWrapperFor(DruidResultSetMetaData.class));
    Assertions.assertTrue(metaData.isWrapperFor(ResultSetMetaData.class));
    Assertions.assertFalse(metaData.isWrapperFor(String.class));

    Assertions.assertSame(metaData, metaData.unwrap(DruidResultSetMetaData.class));
    Assertions.assertSame(metaData, metaData.unwrap(ResultSetMetaData.class));

    Assertions.assertThrows(SQLException.class, () -> metaData.unwrap(String.class));
  }
}
