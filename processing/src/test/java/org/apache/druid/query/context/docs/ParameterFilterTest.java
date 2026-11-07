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

package org.apache.druid.query.context.docs;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
import org.apache.druid.query.context.docs.ParameterDocumentation.StatementType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterFilterTest
{
  private static final ParameterDocumentation NATIVE_GROUP_BY = ParameterDocumentation
      .builder()
      .description("A groupBy parameter.")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .queryType(QueryType.GROUP_BY)
      .build();
  private static final ParameterDocumentation MSQ_AND_DART = ParameterDocumentation
      .builder()
      .description("An MSQ and Dart parameter.")
      .query(Query.SQL)
      .engine(Engine.MSQ, Engine.DART)
      .statement(StatementType.INSERT)
      .build();

  @Test
  void testExactMatch()
  {
    final ParameterFilter filter = ParameterFilter.parse("queryTypes=GROUP_BY");
    assertTrue(filter.matches("p", NATIVE_GROUP_BY));
    assertFalse(filter.matches("p", MSQ_AND_DART));
    assertFalse(ParameterFilter.parse("engines=MSQ").matches("p", MSQ_AND_DART));
    assertTrue(ParameterFilter.parse("engines=MSQ,DART").matches("p", MSQ_AND_DART));
  }

  @Test
  void testEmptyExactMatch()
  {
    final ParameterFilter filter = ParameterFilter.parse("queryTypes=");
    assertFalse(filter.matches("p", NATIVE_GROUP_BY));
    assertTrue(filter.matches("p", MSQ_AND_DART));
  }

  @Test
  void testContains()
  {
    assertTrue(ParameterFilter.parse("engines~MSQ").matches("p", MSQ_AND_DART));
    assertTrue(ParameterFilter.parse("engines~MSQ,DART").matches("p", MSQ_AND_DART));
    assertFalse(ParameterFilter.parse("engines~MSQ,NATIVE").matches("p", MSQ_AND_DART));
  }

  @Test
  void testContainsNone()
  {
    assertTrue(ParameterFilter.parse("engines!~NATIVE").matches("p", MSQ_AND_DART));
    assertFalse(ParameterFilter.parse("engines!~NATIVE").matches("p", NATIVE_GROUP_BY));
    assertFalse(ParameterFilter.parse("engines!~NATIVE,DART").matches("p", MSQ_AND_DART));
  }

  @Test
  void testAllConditionsMustHold()
  {
    final ParameterFilter filter = ParameterFilter.parse("queries=SQL  engines~MSQ statements~INSERT");
    assertTrue(filter.matches("p", MSQ_AND_DART));
    assertFalse(ParameterFilter.parse("queries=SQL statements~SELECT").matches("p", MSQ_AND_DART));
  }

  @Test
  void testNames()
  {
    final ParameterFilter filter = ParameterFilter.parse("names=a,b");
    assertTrue(filter.matches("a", NATIVE_GROUP_BY));
    assertTrue(filter.matches("b", MSQ_AND_DART));
    assertFalse(filter.matches("c", NATIVE_GROUP_BY));
    assertEquals(Set.of("a", "b"), filter.getNames());
    assertTrue(ParameterFilter.parse("names=a engines~NATIVE").matches("a", NATIVE_GROUP_BY));
    assertFalse(ParameterFilter.parse("names=a engines~MSQ").matches("a", NATIVE_GROUP_BY));
    assertFalse(ParameterFilter.parse("names=a engines~NATIVE").matches("b", NATIVE_GROUP_BY));
    assertEquals(Set.of(), ParameterFilter.parse("engines~MSQ").getNames());
  }

  @Test
  void testRemainderMatchesNothingByItself()
  {
    final ParameterFilter filter = ParameterFilter.parse(" * ");
    assertTrue(filter.isRemainder());
    assertFalse(filter.matches("p", NATIVE_GROUP_BY));
    assertFalse(ParameterFilter.parse("engines~NATIVE").isRemainder());
  }

  @Test
  void testRejectsInvalidFilters()
  {
    assertRejected("", "must not be empty");
    assertRejected("engines", "Invalid condition [engines]");
    assertRejected("engine=NATIVE", "Unknown field [engine]");
    assertRejected("engines=native", "Unknown engines value [native]");
    assertRejected("engines~", "has no values");
    assertRejected("engines!~", "has no values");
    assertRejected("engines=MSQ,", "Unknown engines value []");
    assertRejected("names~a", "must use [=]");
    assertRejected("names=", "has an empty name");
    assertRejected("names=a,", "has an empty name");
    assertRejected("engines~MSQ,,DART", "Unknown engines value []");
  }

  private static void assertRejected(final String expression, final String message)
  {
    final IAE exception = assertThrows(IAE.class, () -> ParameterFilter.parse(expression));
    assertTrue(exception.getMessage().contains(message), exception.getMessage());
  }
}
