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

import javax.annotation.Nullable;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.Objects;

/**
 * Represents metadata about a result column, loaded from the {@code sqlTypesHeader} and the native
 * {@code typesHeader}.
 *
 * @param name       name of the column
 * @param type       JDBC column type
 * @param nativeType native Druid type string, e.g. {@code STRING} or {@code ARRAY<LONG>}. Null when the server sends
 *                   no native type for the column, which it does for SQL types that have no Druid equivalent.
 */
public record ColumnMetadata(String name, JDBCType type, @Nullable String nativeType)
{
  public ColumnMetadata
  {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(type, "type");
  }

  /**
   * Convenience constructor for a column without an explicit native type (e.g. synthesized metadata result
   * sets); the Druid native type is derived from the JDBC type.
   */
  public ColumnMetadata(final String name, final JDBCType type)
  {
    this(name, type, nativeTypeForJdbcType(type));
  }

  /**
   * The Druid native type of the elements of {@code nativeType}, when a value of that type is read as an array.
   */
  @Nullable
  public static String arrayElementNativeType(@Nullable final String nativeType)
  {
    if (nativeType == null) {
      return null;
    } else if (nativeType.startsWith("ARRAY<") && nativeType.endsWith(">")) {
      return nativeType.substring("ARRAY<".length(), nativeType.length() - 1).trim();
    } else {
      return nativeType;
    }
  }

  /**
   * The JDBC {@link Types} code for a Druid native type, such as {@link Types#BIGINT} for {@code LONG}.
   */
  public static int jdbcTypeForNativeType(@Nullable final String nativeType)
  {
    return typeForNativeType(nativeType).getVendorTypeNumber();
  }

  /**
   * The {@link JDBCType} for a Druid native type, such as {@link JDBCType#BIGINT} for {@code LONG}. An unknown or
   * absent native type maps to {@link JDBCType#OTHER}.
   */
  public static JDBCType typeForNativeType(@Nullable final String nativeType)
  {
    if (nativeType == null) {
      return JDBCType.OTHER;
    }

    return switch (nativeType) {
      case "LONG" -> JDBCType.BIGINT;
      case "STRING" -> JDBCType.VARCHAR;
      case "DOUBLE" -> JDBCType.DOUBLE;
      case "FLOAT" -> JDBCType.FLOAT;
      default -> nativeType.startsWith("ARRAY") ? JDBCType.ARRAY : JDBCType.OTHER;
    };
  }

  /**
   * The Druid native type name for a {@link JDBCType}, following Druid's SQL-to-native type mapping.
   *
   * @throws IllegalArgumentException if there is no Druid native type for {@code type}
   */
  public static String nativeTypeForJdbcType(final JDBCType type)
  {
    return switch (type) {
      case VARCHAR, CHAR -> "STRING";
      case BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN, TIMESTAMP, DATE -> "LONG";
      case FLOAT, REAL -> "FLOAT";
      case DOUBLE -> "DOUBLE";
      default -> throw new IllegalArgumentException("No Druid type mapping for JDBC type[" + type + "]");
    };
  }

  /**
   * The JDBC {@link Types} code for this column, derived from {@link #type()}, e.g.
   * {@link Types#VARCHAR}.
   */
  public int jdbcType()
  {
    return type.getVendorTypeNumber();
  }

  /**
   * The JDBC {@link Types} code for the elements of this column, when it is read as an array by
   * {@code ResultSet#getArray}. Derived from {@link #nativeType()}.
   */
  public int arrayElementJdbcType()
  {
    return arrayElementType().getVendorTypeNumber();
  }

  /**
   * The {@link JDBCType} of the elements of this column, when it is read as an array by
   * {@code ResultSet#getArray}. Derived from {@link #nativeType()}.
   */
  public JDBCType arrayElementType()
  {
    return typeForNativeType(arrayElementNativeType(nativeType));
  }
}
