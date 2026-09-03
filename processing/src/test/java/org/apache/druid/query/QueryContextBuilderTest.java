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

package org.apache.druid.query;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryContextBuilderTest
{
  @Test
  void testPutRawAndTypedParameters()
  {
    final Map<String, Object> context = QueryContext.builder()
        .putRaw("legacy", 1)
        .put(QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING, 10)
        .put(QueryContextParameters.USE_RESULT_LEVEL_CACHE, false)
        .toMap();

    assertEquals(
        ImmutableMap.of(
            "legacy", 1,
            QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING.getName(), 10,
            QueryContextParameters.USE_RESULT_LEVEL_CACHE.getName(), false
        ),
        context
    );
  }

  @Test
  void testLaterValueReplacesEarlierValue()
  {
    final Map<String, Object> context = QueryContext.builder()
        .put(QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING, 10)
        .putRaw("maxRowsQueuedForOrdering", 20)
        .toMap();

    assertEquals(20, context.get(QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING.getName()));
  }

  @Test
  void testPutAll()
  {
    final Map<String, Object> firstValues = ImmutableMap.of("legacy", 1);
    final Map<String, Object> secondValues = ImmutableMap.of("legacy", 2);
    final Map<String, Object> context = QueryContext.builder()
        .putAll(firstValues)
        .putAll(secondValues)
        .toMap();

    assertEquals(2, context.get("legacy"));
  }

  @Test
  void testTypedPutValidatesParameter()
  {
    assertThrows(
        IAE.class,
        () -> QueryContext.builder().put(QueryContextParameters.MAX_ROWS_QUEUED_FOR_ORDERING, 0)
    );
  }

  @Test
  void testTypedPutAcceptsNullableParameter()
  {
    final Map<String, Object> context = QueryContext.builder()
        .put(QueryContextParameters.USE_RESULT_LEVEL_CACHE, null)
        .toMap();

    assertTrue(context.containsKey(QueryContextParameters.USE_RESULT_LEVEL_CACHE.getName()));
    assertNull(context.get(QueryContextParameters.USE_RESULT_LEVEL_CACHE.getName()));
  }

  @Test
  void testToContext()
  {
    final QueryContext context = QueryContext.builder()
        .put(QueryContextParameters.USE_RESULT_LEVEL_CACHE, false)
        .toContext();

    assertFalse(context.isUseResultLevelCache());
  }
}
