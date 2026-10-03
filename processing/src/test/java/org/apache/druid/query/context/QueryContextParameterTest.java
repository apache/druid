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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
import org.apache.druid.query.context.docs.ParameterDocumentation.StatementType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.druid.query.context.constraint.Range.closedRange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryContextParameterTest
{
  @Test
  void testDescriptorMetadataAndParsing()
  {
    final QueryContextParameter<Integer> parameter = QueryContextParameter
        .builder("maxThings", Integer.class, value -> QueryContexts.getAsInt("maxThings", value))
        .constraint(closedRange(0, Integer.MAX_VALUE))
        .defaultValue(10)
        .description("Maximum number of things.")
        .query(Query.JSON)
        .engine(Engine.NATIVE)
        .queryType(QueryType.SCAN)
        .statement(StatementType.SELECT)
        .defaultDescription("runtime configuration")
        .since("39.0.0")
        .build();

    assertEquals("maxThings", parameter.getName());
    assertEquals(Integer.class, parameter.getValueType());
    assertEquals(12, parameter.parse("12"));
    assertEquals(10, parameter.getDefaultValue().orElseThrow());
    assertTrue(parameter.isNullable());
    assertFalse(parameter.isDeprecated());
    assertTrue(parameter.getDeprecationMessage().isEmpty());
    final ParameterDocumentation docs = parameter.getDocumentation().orElseThrow();
    assertEquals("39.0.0", docs.getSince().orElseThrow());
    assertEquals("Maximum number of things.", docs.getDescription());
    assertEquals(Set.of(Query.JSON), docs.getQueries());
    assertEquals(Set.of(Engine.NATIVE), docs.getEngines());
    assertEquals(Set.of(QueryType.SCAN), docs.getQueryTypes());
    assertEquals(Set.of(StatementType.SELECT), docs.getStatementTypes());
    assertEquals("runtime configuration", docs.getDefaultDescription().orElseThrow());
    final Range.Constraint<?> constraint = (Range.Constraint<?>) parameter.getConstraints().get(0);
    assertTrue(constraint.getClass().isAnonymousClass());
    assertEquals(0, constraint.getLowerBound());
    assertEquals(Integer.MAX_VALUE, constraint.getUpperBound());
    assertEquals("maxThings", parameter.toString());
  }

  @Test
  void testValidatorRejectsParsedValue()
  {
    final QueryContextParameter<Integer> parameter = QueryContextParameter
        .builder("maxThings", Integer.class, value -> (Integer) value)
        .constraint(closedRange(0, 10))
        .build();

    assertThrows(IAE.class, () -> parameter.parse(-1));
    assertThrows(IAE.class, () -> parameter.validate(-1));
    assertThrows(IAE.class, () -> parameter.validate(11));
    parameter.validate(0);
    parameter.validate(10);
  }

  @Test
  void testValidateDoesNotInvokeParser()
  {
    final AtomicBoolean parserCalled = new AtomicBoolean();
    final QueryContextParameter<Integer> parameter = QueryContextParameter
        .builder(
            "maxThings",
            Integer.class,
            value -> {
              parserCalled.set(true);
              return QueryContexts.getAsInt("maxThings", value);
            }
        )
        .build();

    assertEquals(1, parameter.validate(1));
    assertFalse(parserCalled.get());

    assertEquals(1, parameter.parse(1));
    assertFalse(parserCalled.get());

    assertEquals(1, parameter.parse("1"));
    assertTrue(parserCalled.get());
  }

  @Test
  void testParseOrDefault()
  {
    final QueryContextParameter<Integer> parameter = QueryContextParameter
        .builder("maxThings", Integer.class, value -> QueryContexts.getAsInt("maxThings", value))
        .defaultValue(10)
        .build();

    assertEquals(12, parameter.parseOrDefault("12"));
    assertEquals(10, parameter.parseOrDefault(null));
  }

  @Test
  void testParseOrDefaultRequiresDeclaredDefault()
  {
    final QueryContextParameter<String> parameter = QueryContextParameter
        .builder("tag", String.class, value -> (String) value)
        .build();

    assertThrows(ISE.class, () -> parameter.parseOrDefault(null));
  }

  @Test
  void testRangeRejectsInvalidBounds()
  {
    assertThrows(IAE.class, () -> closedRange(1, 0));
  }

  @Test
  void testNullableValueSkipsConstraints()
  {
    final AtomicBoolean constraintCalled = new AtomicBoolean();
    final QueryContextParameter<Integer> parameter = QueryContextParameter
        .builder("required", Integer.class, value -> (Integer) value)
        .constraint((parameterName, value) -> constraintCalled.set(true))
        .build();

    assertNull(parameter.validate(null));
    assertFalse(constraintCalled.get());
    parameter.validate(1);
    assertTrue(constraintCalled.get());
  }

  @Test
  void testNonNullableParameterRejectsNull()
  {
    final QueryContextParameter<String> parameter = QueryContextParameter
        .builder("required", String.class, String::valueOf)
        .nullable(false)
        .build();
    final Map<String, Object> context = new HashMap<>();

    assertFalse(parameter.isNullable());
    assertThrows(IAE.class, () -> parameter.parse(null));
    assertThrows(IAE.class, () -> parameter.validate(null));
    assertThrows(IAE.class, () -> parameter.set(context, null));
    assertTrue(context.isEmpty());

    final QueryContextParameter<String> nullProducingParser = QueryContextParameter
        .builder("required", String.class, ignored -> null)
        .nullable(false)
        .build();
    assertThrows(IAE.class, () -> nullProducingParser.parse(42));
  }

  @Test
  void testSet()
  {
    final QueryContextParameter<String> parameter = QueryContextParameter
        .builder("tag", String.class, String::valueOf)
        .build();
    final Map<String, Object> context = new HashMap<>();

    parameter.set(context, "value");

    assertEquals(Map.of("tag", "value"), context);
  }

  @Test
  void testValidatorRejectsDefaultValueAtBuildTime()
  {
    final QueryContextParameter.Builder<Integer> builder = QueryContextParameter
        .builder("maxThings", Integer.class, value -> (Integer) value)
        .constraint(closedRange(0, Integer.MAX_VALUE))
        .defaultValue(-1);

    assertThrows(IAE.class, builder::build);
  }

  @Test
  void testDefaultsApplyToOptionalMetadata()
  {
    final QueryContextParameter<String> parameter = QueryContextParameter
        .builder("tag", String.class, value -> String.valueOf(value))
        .deprecated("Use `newTag` instead.")
        .build();

    assertTrue(parameter.isDeprecated());
    assertEquals("Use `newTag` instead.", parameter.getDeprecationMessage().orElseThrow());
    assertTrue(parameter.getDefaultValue().isEmpty());
    assertTrue(parameter.getDocumentation().isEmpty());
  }

  @Test
  void testRejectsInvalidName()
  {
    assertThrows(
        IAE.class,
        () -> QueryContextParameter.builder(" parameter", String.class, value -> String.valueOf(value))
    );
  }

  @Test
  void testRejectsBlankDeprecationMessage()
  {
    final QueryContextParameter.Builder<String> builder = QueryContextParameter
        .builder("parameter", String.class, String::valueOf);

    assertThrows(IAE.class, () -> builder.deprecated(" "));
  }
}
