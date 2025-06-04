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

import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CalcitesTest extends CalciteTestBase
{
  @Test
  public void testEscapeStringLiteral()
  {
    assertEquals("''", Calcites.escapeStringLiteral(""));
    assertEquals("'foo'", Calcites.escapeStringLiteral("foo"));
    assertEquals("'foo bar'", Calcites.escapeStringLiteral("foo bar"));
    assertEquals("U&'foö bar'", Calcites.escapeStringLiteral("foö bar"));
    assertEquals("U&'foo \\0026\\0026 bar'", Calcites.escapeStringLiteral("foo && bar"));
    assertEquals("U&'foo \\005C bar'", Calcites.escapeStringLiteral("foo \\ bar"));
    assertEquals("U&'foo\\0027s bar'", Calcites.escapeStringLiteral("foo's bar"));
    assertEquals("U&'друид'", Calcites.escapeStringLiteral("друид"));
  }

  @Test
  public void testFindUnusedPrefix()
  {
    assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar")));
    assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x")));
    assertEquals("_x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x0")));
    assertEquals("_x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x4")));
    assertEquals("__x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", "x0")));
    assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", " x")));
    assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "_xbxx")));
    assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x")));
    assertEquals("__x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "x1a", "_x90")));
  }

  @Test
  public void testGetStringComparatorForRelDataType()
  {
    for (final SqlTypeName typeName : SqlTypeFamily.CHARACTER.getTypeNames()) {
      final RelDataType type = DruidTypeSystem.TYPE_FACTORY.createSqlType(typeName);
      assertEquals(
          StringComparators.LEXICOGRAPHIC,
          Calcites.getStringComparatorForRelDataType(type),
          type.getFullTypeString()
      );
    }

    for (final SqlTypeName typeName : SqlTypeFamily.NUMERIC.getTypeNames()) {
      final RelDataType type = DruidTypeSystem.TYPE_FACTORY.createSqlType(typeName);
      assertEquals(
          StringComparators.NUMERIC,
          Calcites.getStringComparatorForRelDataType(type),
          type.getFullTypeString()
      );
    }

    assertEquals(
        StringComparators.NATURAL,
        Calcites.getStringComparatorForRelDataType(
            RowSignatures.makeComplexType(DruidTypeSystem.TYPE_FACTORY, ColumnType.UNKNOWN_COMPLEX, false)
        ),
        ColumnType.UNKNOWN_COMPLEX.toString()
    );

    assertEquals(
        StringComparators.NATURAL,
        Calcites.getStringComparatorForRelDataType(
            RowSignatures.makeComplexType(DruidTypeSystem.TYPE_FACTORY, ColumnType.NESTED_DATA, false)
        ),
        ColumnType.NESTED_DATA.toString()
    );

    final RelDataType timestampType = DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    assertEquals(
        StringComparators.NUMERIC,
        Calcites.getStringComparatorForRelDataType(timestampType),
        timestampType.getFullTypeString()
    );

    final RelDataType dateType = DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    assertEquals(
        StringComparators.NUMERIC,
        Calcites.getStringComparatorForRelDataType(dateType),
        dateType.getFullTypeString()
    );

    final RelDataType bigintArrayType =
        DruidTypeSystem.TYPE_FACTORY.createArrayType(
            DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            -1
        );
    assertEquals(
        StringComparators.NATURAL,
        Calcites.getStringComparatorForRelDataType(bigintArrayType),
        bigintArrayType.getFullTypeString()
    );

    final RelDataType booleanType = DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    assertEquals(
        StringComparators.NUMERIC,
        Calcites.getStringComparatorForRelDataType(booleanType),
        booleanType.getFullTypeString()
    );

    final RelDataType otherType = DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.OTHER);
    assertEquals(
        StringComparators.NATURAL,
        Calcites.getStringComparatorForRelDataType(otherType),
        otherType.getFullTypeString()
    );

    final RelDataType nullType = DruidTypeSystem.TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    assertEquals(
        StringComparators.NATURAL,
        Calcites.getStringComparatorForRelDataType(nullType),
        nullType.getFullTypeString()
    );
  }

  @Test
  public void testGetStringComparatorForColumnType()
  {
    assertEquals(StringComparators.LEXICOGRAPHIC, Calcites.getStringComparatorForValueType(ColumnType.STRING));
    assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.LONG));
    assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.FLOAT));
    assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.DOUBLE));
    assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.STRING_ARRAY));
    assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.LONG_ARRAY));
    assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.DOUBLE_ARRAY));
    assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.NESTED_DATA));
    assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.UNKNOWN_COMPLEX));
  }
}
