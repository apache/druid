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

package org.apache.druid.query.context;

import org.apache.druid.query.BadQueryContextException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryContextParameterValidatorTest
{
  @Test
  void testValidateValues()
  {
    assertInvalid(
        Map.of("maxRowsQueuedForOrdering", -1),
        false,
        "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [-1]"
    );

    assertInvalid(
        Map.of("maxRowsQueuedForOrdering", "not-an-int"),
        false,
        "Query context parameter [maxRowsQueuedForOrdering] should be in integer format, but got [not-an-int]"
    );

    assertInvalid(
        Map.of("useResultLevelCache", 2),
        false,
        "Query context parameter [useResultLevelCache] should be a boolean, but got [2]"
    );

    QueryContextParameterValidator.validate(Map.of("maxRowsQueuedForOrdering", Integer.MAX_VALUE), true);
    QueryContextParameterValidator.validate(Collections.singletonMap("maxRowsQueuedForOrdering", null), true);
    QueryContextParameterValidator.validate(Map.of(), true);
  }

  @Test
  void testValidateIgnoresUnknownParametersUnlessRejected()
  {
    QueryContextParameterValidator.validate(Map.of("maxRowsQueuedForOrdering", 1, "myTraceId", -1), false);
    // Values are validated whether or not unknown parameters are rejected.
    assertInvalid(
        Map.of("maxRowsQueuedForOrdering", 0, "myTraceId", -1),
        false,
        "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [0]"
    );
  }

  @Test
  void testRejectUnknownParameterWithHint()
  {
    assertInvalid(
        Map.of("timeout", 1000, "useCahce", false),
        true,
        "Unknown query context parameter [useCahce]. Did you mean [useCache]?"
    );
  }

  @Test
  void testRejectUnknownParameterWithoutHint()
  {
    assertInvalid(Map.of("myTraceId", "abc-123"), true, "Unknown query context parameter [myTraceId].");
  }

  @Test
  void testRejectUnknownParameterWithNullValue()
  {
    assertInvalid(
        Collections.singletonMap("useCahce", null),
        true,
        "Unknown query context parameter [useCahce]. Did you mean [useCache]?"
    );
  }

  @Test
  void testParameterNamesAreCaseSensitive()
  {
    assertInvalid(
        Map.of("usecache", false),
        true,
        "Unknown query context parameter [usecache]. Did you mean [useCache]?"
    );
  }

  @Test
  void testValidateReportsAlphabeticallyFirstInvalidParameter()
  {
    final Map<String, Object> unknownNames = new LinkedHashMap<>();
    unknownNames.put("useCahce", false);
    unknownNames.put("useCache", false);
    unknownNames.put("myTraceId", "abc-123");
    assertInvalid(unknownNames, true, "Unknown query context parameter [myTraceId].");

    // An unknown name and an invalid value are reported in the same alphabetical order.
    final Map<String, Object> unknownNameFirst = new LinkedHashMap<>();
    unknownNameFirst.put("maxRowsQueuedForOrdering", 0);
    unknownNameFirst.put("aTraceId", "abc-123");
    assertInvalid(unknownNameFirst, true, "Unknown query context parameter [aTraceId].");

    final Map<String, Object> invalidValueFirst = new LinkedHashMap<>();
    invalidValueFirst.put("zTraceId", "abc-123");
    invalidValueFirst.put("maxRowsQueuedForOrdering", 0);
    assertInvalid(
        invalidValueFirst,
        true,
        "Query context parameter [maxRowsQueuedForOrdering] must be within the range [1, 2147483647], but was [0]"
    );
  }

  @Test
  void testGetHints()
  {
    final List<QueryContextParameter<?>> candidates = List.of(
        parameter("cardinalities").build(),
        parameter("cardinality").build(),
        parameter("cardinalityLimit").build(),
        parameter("carinality").build(),
        parameter("cardinalyti").build()
    );

    // Closest first, then by name, at most three.
    assertEquals(
        List.of("cardinality", "cardinalyti", "carinality"),
        QueryContextParameterValidator.getHints("cardinalty", candidates)
    );
    // Case-insensitive.
    assertEquals(
        List.of("cardinality", "carinality", "cardinalyti"),
        QueryContextParameterValidator.getHints("CARDINALITY", candidates)
    );
    // At most one edit per three characters: "cxrdxnxlxtx" is five substitutions away from "cardinality".
    assertEquals(List.of(), QueryContextParameterValidator.getHints("cxrdxnxlxtx", candidates));
    // Three insertions are within the budget of "cardinal", but the names differ in length by more than a third.
    assertEquals(List.of(), QueryContextParameterValidator.getHints("cardinal", candidates));
  }

  @Test
  void testGetHintsSkipsSystemAndDeprecatedParameters()
  {
    final List<QueryContextParameter<?>> candidates = List.of(
        parameter("systemParameter").visibility(QueryContextParameter.Visibility.SYSTEM).build(),
        parameter("deprecatedParameter").deprecated("Use another parameter.").build()
    );

    assertEquals(List.of(), QueryContextParameterValidator.getHints("systemParameters", candidates));
    assertEquals(List.of(), QueryContextParameterValidator.getHints("deprecatedParameters", candidates));
  }

  private static QueryContextParameter.Builder<String> parameter(final String name)
  {
    return QueryContextParameters.stringParameter(name).description("Test parameter.");
  }

  /**
   * Asserts that validating the parameters fails with the expected message.
   */
  private static void assertInvalid(
      final Map<String, Object> parameters,
      final boolean rejectUnknownParameters,
      final String expectedMessage
  )
  {
    final BadQueryContextException e = assertThrows(
        BadQueryContextException.class,
        () -> QueryContextParameterValidator.validate(parameters, rejectUnknownParameters)
    );
    assertEquals(expectedMessage, e.getMessage());
  }
}
