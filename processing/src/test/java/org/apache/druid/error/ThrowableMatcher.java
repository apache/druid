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
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ThrowableMatcher
{
  public static ThrowableMatcher of(final Class<? extends Throwable> expectedType)
  {
    return new ThrowableMatcher(expectedType);
  }

  private final Class<? extends Throwable> expectedType;
  private final List<Consumer<Throwable>> assertions;

  public ThrowableMatcher(final Class<? extends Throwable> expectedType)
  {
    this.expectedType = expectedType;
    assertions = new ArrayList<>();
  }

  public ThrowableMatcher expectMessageIs(final String message)
  {
    assertions.add(exception -> Assertions.assertEquals(message, exception.getMessage()));
    return this;
  }

  public ThrowableMatcher expectMessageContains(final String contains)
  {
    assertions.add(exception -> {
      Assertions.assertNotNull(exception.getMessage());
      Assertions.assertTrue(exception.getMessage().contains(contains));
    });
    return this;
  }

  public ThrowableMatcher expectMessage(final Predicate<String> messageMatcher)
  {
    assertions.add(exception -> {
      final String message = exception.getMessage();
      Assertions.assertNotNull(message);
      Assertions.assertTrue(messageMatcher.test(message));
    });
    return this;
  }

  public ThrowableMatcher expectCause(final Predicate<Throwable> causeMatcher)
  {
    assertions.add(exception -> Assertions.assertTrue(causeMatcher.test(exception.getCause())));
    return this;
  }

  public static void assertThat(final Throwable actual, final ThrowableMatcher expected)
  {
    final Throwable exception = Assertions.assertInstanceOf(expected.expectedType, actual);
    expected.assertions.forEach(assertion -> assertion.accept(exception));
  }

  public void assertThat(final Throwable actual)
  {
    assertThat(actual, this);
  }

  public void assertThrowsAndMatches(final ThrowingSupplier fn)
  {
    final Throwable exception = Assertions.assertThrows(expectedType, fn::get);
    assertThat(exception, this);
  }

  public interface ThrowingSupplier
  {
    void get() throws Throwable;
  }
}
