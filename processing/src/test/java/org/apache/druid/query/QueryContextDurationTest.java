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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.context.QueryContextParameterValidator;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class QueryContextDurationTest
{
  @Test
  public void testDurationFormsAreEquivalent()
  {
    for (Object value : new Object[]{30_000L, "30000", "30s", "PT30S", "  30s  "}) {
      final QueryContext context = QueryContext.of(Map.of("timeout", value));
      Assertions.assertEquals(30_000L, context.getTimeout(1L), String.valueOf(value));
      Assertions.assertEquals(30_000L, context.asMap().get("timeout"), String.valueOf(value));
    }
  }

  @Test
  public void testSubMillisecondTimeoutRoundsUp()
  {
    final QueryContext context = QueryContext.of(Map.of("timeout", "250us"));
    Assertions.assertEquals(1L, context.getTimeout(0L));
    Assertions.assertEquals(1L, context.asMap().get("timeout"));
  }

  @Test
  public void testSerializationForOlderNodesUsesMilliseconds() throws Exception
  {
    final QueryContext context = QueryContext.of(Map.of("timeout", "30s"));
    Assertions.assertEquals("{\"timeout\":30000}", new ObjectMapper().writeValueAsString(context));
  }

  @Test
  public void testOverridesAreNormalized()
  {
    final QueryContext original = QueryContext.of(Map.of("timeout", "10s"));
    final QueryContext updated = original.override(Map.of("timeout", "PT1.5S"));
    Assertions.assertEquals(10_000L, original.getTimeout(0L));
    Assertions.assertEquals(1_500L, updated.getTimeout(0L));
    Assertions.assertEquals(1_500L, updated.asMap().get("timeout"));
  }

  @Test
  public void testLegacyLargeTimeoutRemainsAccepted()
  {
    for (Object value : new Object[]{Long.MAX_VALUE, String.valueOf(Long.MAX_VALUE)}) {
      Assertions.assertEquals(Long.MAX_VALUE, QueryContext.of(Map.of("timeout", value)).getTimeout(0L));
    }
  }

  @Test
  public void testInvalidDurationsAreRejected()
  {
    for (String value : new String[]{"-5s", "1.5h", "P1M", "30S", "9999999999d"}) {
      final QueryContext context = QueryContext.of(Map.of("timeout", value));
      Assertions.assertThrows(BadQueryContextException.class, () -> context.getTimeout(0L), value);
    }
  }

  @Test
  public void testValidationAndMaxTimeoutUseParsedMilliseconds()
  {
    QueryContextParameterValidator.validate(Map.of("timeout", "30s"), false);
    Assertions.assertEquals(30_000L, QueryContextParameters.TIMEOUT.parse("30s"));
    final QueryContext context = QueryContext.of(Map.of("timeout", "30s"));
    Assertions.assertDoesNotThrow(() -> context.verifyMaxQueryTimeout(30_000L));
    Assertions.assertThrows(BadQueryContextException.class, () -> context.verifyMaxQueryTimeout(29_999L));
  }

  @Test
  public void testInvalidRawValueIsRetainedUntilValidation()
  {
    final QueryContext context = QueryContext.of(Map.of("timeout", "not-a-duration"));
    Assertions.assertEquals("not-a-duration", context.asMap().get("timeout"));
    Assertions.assertThrows(BadQueryContextException.class, () -> context.getTimeout(0L));
  }

  @Test
  public void testMinuteDurationOverflowBoundary()
  {
    Assertions.assertEquals(
        9_223_372_020_000L,
        QueryContext.of(
            Map.of("timeout", "153722867m")
        ).getTimeout(0L)
    );

    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> QueryContext.of(
            Map.of("timeout", "153722868m")
        ).getTimeout(0L)
    );
  }
}
