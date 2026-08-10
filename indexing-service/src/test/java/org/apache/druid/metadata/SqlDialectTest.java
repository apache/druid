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

package org.apache.druid.metadata;

import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class SqlDialectTest
{
  private static final List<String> EQUALITY_VALUES = List.of(
      "CaseSensitive",
      "casesensitive",
      "café",
      "cafe\u0301",
      "trailing-space ",
      "",
      "%_wildcards",
      "backslash\\escape"
  );

  /** Equality and IN remain eligible for every configured metadata database across sensitive string values. */
  @ParameterizedTest(name = "{0}: equality/IN value [{1}]")
  @MethodSource("dialectAndEqualityValues")
  public void testEqualityAndInCapabilities(
      final SqlDialect dialect,
      final String value
  )
  {
    Assertions.assertTrue(dialect.supports(new SelectorDimFilter("id", value, null)));
    Assertions.assertTrue(dialect.supports(new EqualityFilter("id", ColumnType.STRING, value, null)));
    Assertions.assertTrue(dialect.supports(new InDimFilter("id", List.of(value, "other"), null)));
    Assertions.assertEquals(
        " AND id = :task_filter_0_0 ",
        new SqlPredicateBuilder(
            null,
            List.of(new SelectorDimFilter("id", value, null)),
            dialect
        ).getSql()
    );
  }

  /** Null equality is not eligible for metadata SQL and remains a Broker residual filter. */
  @ParameterizedTest
  @EnumSource(SqlDialect.class)
  public void testNullEqualityIsDeclined(final SqlDialect dialect)
  {
    Assertions.assertFalse(dialect.supports(new SelectorDimFilter("id", null, null)));
  }

  /** An unknown metadata database declines even equality rather than risking an unsafe predicate. */
  @Test
  public void testUnknownDialectDeclinesEquality()
  {
    Assertions.assertFalse(
        SqlDialect.NONE.supports(new SelectorDimFilter("id", "task-1", null))
    );
  }

  private static Stream<Arguments> dialectAndEqualityValues()
  {
    return Stream.of(
        SqlDialect.DERBY,
        SqlDialect.MYSQL,
        SqlDialect.POSTGRESQL,
        SqlDialect.SQL_SERVER
    )
                 .flatMap(dialect -> EQUALITY_VALUES.stream().map(value -> Arguments.of(dialect, value)));
  }
}
