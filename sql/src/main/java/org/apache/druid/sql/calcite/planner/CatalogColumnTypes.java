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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

/**
 * Converts a SQL type as written in a statement into the type string stored in the catalog. Calcite accepts any
 * type spelling; Druid accepts only its own supported types and their aliases, plus the {@code TYPE('...')} escape
 * hatch for native type strings that have no SQL spelling, such as {@code COMPLEX<json>}.
 * <p>
 * Druid has its own rules for nullability, so any nullability clause is ignored.
 */
public class CatalogColumnTypes
{
  /**
   * Which statement the type was written in. The two differ in what they accept, so they are named rather than
   * flagged.
   */
  private enum Target
  {
    /**
     * The {@code EXTEND} clause, describing columns read from an external input source. Those are read as their
     * underlying storage type, so {@code TIMESTAMP} is not among them, and the escape hatch is limited to complex
     * types.
     */
    EXTERNAL,

    /**
     * A catalog DDL statement, which additionally accepts {@code TIMESTAMP} (how {@code __time} is spelled in SQL)
     * and any native type string through the escape hatch.
     */
    CATALOG
  }

  private CatalogColumnTypes()
  {
    // No instantiation.
  }

  public static String forExternalColumn(String name, SqlDataTypeSpec dataType)
  {
    return convert(name, dataType, Target.EXTERNAL);
  }

  public static String forCatalogColumn(String name, SqlDataTypeSpec dataType)
  {
    final String typeString = convert(name, dataType, Target.CATALOG);
    // the catalog rejects unparseable types at write time, but catching it here attributes the error to the statement
    // rather than to a Coordinator round trip.
    if (Columns.druidTypeFromString(typeString) == null) {
      throw unsupportedType(name, dataType);
    }
    return typeString;
  }

  private static String convert(String name, SqlDataTypeSpec dataType, Target target)
  {
    final SqlTypeNameSpec spec = dataType.getTypeNameSpec();
    if (spec == null) {
      throw unsupportedType(name, dataType);
    }
    final SqlIdentifier typeNameIdentifier = spec.getTypeName();
    if (typeNameIdentifier == null || !typeNameIdentifier.isSimple()) {
      throw unsupportedType(name, dataType);
    }
    final String simpleName = typeNameIdentifier.getSimple();

    if (spec instanceof SqlUserDefinedTypeNameSpec) {
      // The TYPE('...') escape hatch names a Druid native type. Parse and validate rather than passing the raw
      // string downstream, where a malformed type string would silently resolve to a different type, and return the
      // canonical form.
      if (target == Target.EXTERNAL && !StringUtils.toLowerCase(simpleName).startsWith("complex<")) {
        throw unsupportedType(name, dataType);
      }
      final ColumnType nativeType = ColumnType.fromString(simpleName);
      if (nativeType == null) {
        throw unsupportedType(name, dataType);
      }
      return nativeType.asTypeString();
    }

    final SqlTypeName type = SqlTypeName.get(simpleName);
    if (type == null) {
      throw unsupportedType(name, dataType);
    }
    if (SqlTypeName.CHAR_TYPES.contains(type)) {
      return SqlTypeName.VARCHAR.name();
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      return SqlTypeName.BIGINT.name();
    }
    switch (type) {
      case DOUBLE:
        return SqlType.DOUBLE.name();
      case FLOAT:
      case REAL:
        return SqlType.FLOAT.name();
      case ARRAY:
        return convert(name, dataType.getComponentTypeSpec(), target) + " " + SqlType.ARRAY.name();
      case TIMESTAMP:
        if (target == Target.CATALOG) {
          return Columns.SQL_TIMESTAMP;
        }
        throw unsupportedType(name, dataType);
      default:
        throw unsupportedType(name, dataType);
    }
  }

  private static RuntimeException unsupportedType(String name, SqlDataTypeSpec dataType)
  {
    return new IAE(StringUtils.format(
        "Column [%s] has an unsupported type: [%s]",
        name,
        dataType
    ));
  }
}
