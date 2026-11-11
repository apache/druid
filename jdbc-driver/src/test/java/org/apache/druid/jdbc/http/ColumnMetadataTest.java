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

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.JDBCType;


public class ColumnMetadataTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ColumnMetadata.class)
                  .suppress(Warning.NULL_FIELDS)
                  .verify();
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({
      "VARCHAR,   STRING",
      "CHAR,      STRING",
      "BIGINT,    LONG",
      "INTEGER,   LONG",
      "SMALLINT,  LONG",
      "TINYINT,   LONG",
      "BOOLEAN,   LONG",
      "TIMESTAMP, LONG",
      "DATE,      LONG",
      "FLOAT,     FLOAT",
      "DOUBLE,    DOUBLE"
  })
  public void testTwoArgConstructorDerivesNativeType(final JDBCType type, final String expectedNativeType)
  {
    final ColumnMetadata columnMetadata = new ColumnMetadata("c", type);

    Assertions.assertEquals("c", columnMetadata.name());
    Assertions.assertEquals(type, columnMetadata.type());
    Assertions.assertEquals(expectedNativeType, columnMetadata.nativeType());
    Assertions.assertEquals(type.getVendorTypeNumber(), columnMetadata.jdbcType());
  }

  @Test
  public void testTwoArgConstructorRejectsTypeWithoutNativeMapping()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ColumnMetadata("c", JDBCType.ARRAY));
  }

  /**
   * Anything that is not a known scalar or an ARRAY is OTHER.
   */
  @ParameterizedTest(name = "{0} -> {1}")
  @CsvSource({
      "LONG,             BIGINT",
      "STRING,           VARCHAR",
      "DOUBLE,           DOUBLE",
      "FLOAT,            FLOAT",
      "ARRAY<LONG>,      ARRAY",
      "ARRAY<STRING>,    ARRAY",
      "COMPLEX<json>,    OTHER",
      "COMPLEX<hyperUnique>, OTHER"
  })
  public void testTypeForNativeType(final String nativeType, final JDBCType expectedType)
  {
    Assertions.assertEquals(expectedType, ColumnMetadata.typeForNativeType(nativeType));
    Assertions.assertEquals(expectedType.getVendorTypeNumber(), ColumnMetadata.jdbcTypeForNativeType(nativeType));
  }

  /**
   * A scalar native type is its own element type, which is what a multi-value string dimension looks like.
   */
  @ParameterizedTest(name = "{0} -> {1}/{2}")
  @CsvSource({
      "ARRAY<LONG>,        LONG,        BIGINT",
      "ARRAY<STRING>,      STRING,      VARCHAR",
      "ARRAY<DOUBLE>,      DOUBLE,      DOUBLE",
      "ARRAY<FLOAT>,       FLOAT,       FLOAT",
      "ARRAY<ARRAY<LONG>>, ARRAY<LONG>, ARRAY",
      "ARRAY<COMPLEX<json>>, COMPLEX<json>, OTHER",
      "STRING,             STRING,      VARCHAR"
  })
  public void testArrayElementType(
      final String nativeType,
      final String expectedElementNativeType,
      final JDBCType expectedElementType
  )
  {
    final ColumnMetadata columnMetadata = new ColumnMetadata("c", JDBCType.ARRAY, nativeType);

    Assertions.assertEquals(expectedElementNativeType, ColumnMetadata.arrayElementNativeType(nativeType));
    Assertions.assertEquals(expectedElementType, columnMetadata.arrayElementType());
    Assertions.assertEquals(expectedElementType.getVendorTypeNumber(), columnMetadata.arrayElementJdbcType());
  }

  @Test
  public void testConstructorRejectsNulls()
  {
    Assertions.assertThrows(NullPointerException.class, () -> new ColumnMetadata(null, JDBCType.VARCHAR));
    Assertions.assertThrows(NullPointerException.class, () -> new ColumnMetadata("test_column", null));
  }

  @Test
  public void testNullNativeType()
  {
    final ColumnMetadata columnMetadata = new ColumnMetadata("test_column", JDBCType.OTHER, null);

    Assertions.assertNull(columnMetadata.nativeType());
    Assertions.assertEquals(JDBCType.OTHER, columnMetadata.arrayElementType());
    Assertions.assertEquals(JDBCType.OTHER, ColumnMetadata.typeForNativeType(null));
    Assertions.assertNull(ColumnMetadata.arrayElementNativeType(null));
  }
}
