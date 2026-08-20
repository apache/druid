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

package org.apache.druid.error;

import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class DruidExceptionMatcher
{
  public static DruidExceptionMatcher invalidInput()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.USER,
        DruidException.Category.INVALID_INPUT,
        "invalidInput"
    );
  }

  public static DruidExceptionMatcher notFound()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.USER,
        DruidException.Category.NOT_FOUND,
        "notFound"
    );
  }

  public static DruidExceptionMatcher unsupported()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.UNSUPPORTED,
        "general"
    );
  }

  public static DruidExceptionMatcher invalidSqlInput()
  {
    return invalidInput().expectContext("sourceType", "sql");
  }

  public static DruidExceptionMatcher internalServerError()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.RUNTIME_FAILURE,
        "internalServerError"
    );
  }

  public static DruidExceptionMatcher forbidden()
  {
    return new DruidExceptionMatcher(DruidException.Persona.USER, DruidException.Category.FORBIDDEN, "general");
  }

  public static DruidExceptionMatcher conflict()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.CONFLICT,
        "general"
    );
  }

  public static DruidExceptionMatcher defensive()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.DEVELOPER,
        DruidException.Category.DEFENSIVE,
        "general"
    );
  }

  private final List<Consumer<DruidException>> assertions;

  public DruidExceptionMatcher(
      final DruidException.Persona targetPersona,
      final DruidException.Category category,
      final String errorCode
  )
  {
    assertions = new ArrayList<>();
    assertions.add(exception -> Assertions.assertEquals(targetPersona, exception.getTargetPersona()));
    assertions.add(exception -> Assertions.assertEquals(category, exception.getCategory()));
    assertions.add(exception -> Assertions.assertEquals(errorCode, exception.getErrorCode()));
  }

  public DruidExceptionMatcher expectContext(final String key, final String value)
  {
    assertions.add(exception -> {
      final Map<String, String> context = exception.getContext();
      Assertions.assertTrue(context.containsKey(key));
      Assertions.assertEquals(value, context.get(key));
    });
    return this;
  }

  public DruidExceptionMatcher expectMessageIs(final String s)
  {
    assertions.add(exception -> Assertions.assertEquals(s, exception.getMessage()));
    return this;
  }

  public DruidExceptionMatcher expectMessageContains(final String contains)
  {
    assertions.add(exception -> {
      Assertions.assertNotNull(exception.getMessage());
      Assertions.assertTrue(exception.getMessage().contains(contains));
    });
    return this;
  }

  public DruidExceptionMatcher expectMessage(final Predicate<String> messageMatcher)
  {
    assertions.add(exception -> Assertions.assertTrue(messageMatcher.test(exception.getMessage())));
    return this;
  }

  public DruidExceptionMatcher expectException(final Predicate<Throwable> causeMatcher)
  {
    assertions.add(exception -> Assertions.assertTrue(causeMatcher.test(exception.getCause())));
    return this;
  }

  public static void assertThat(final Throwable actual, final DruidExceptionMatcher expected)
  {
    final DruidException exception = Assertions.assertInstanceOf(DruidException.class, actual);
    expected.assertions.forEach(assertion -> assertion.accept(exception));
  }

  public static void assertThat(
      final String reason,
      final Throwable actual,
      final DruidExceptionMatcher expected
  )
  {
    try {
      assertThat(actual, expected);
    }
    catch (AssertionError e) {
      throw new AssertionError(reason + ": " + e.getMessage(), e);
    }
  }

  public void assertThrowsAndMatches(final ThrowingSupplier fn)
  {
    final DruidException exception = Assertions.assertThrows(DruidException.class, fn::get);
    assertThat(exception, this);
  }

  public interface ThrowingSupplier
  {
    void get();
  }
}
