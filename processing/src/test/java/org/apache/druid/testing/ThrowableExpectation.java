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

package org.apache.druid.testing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class ThrowableExpectation implements InvocationInterceptor
{
  private Class<? extends Throwable> expectedType;
  private String expectedMessage;
  private boolean messageStartsWith;
  private final List<Predicate<Throwable>> predicates = new ArrayList<>();

  public static ThrowableExpectation none()
  {
    return new ThrowableExpectation();
  }

  public void expect(final Class<? extends Throwable> type)
  {
    expectedType = type;
  }

  public void expect(final Predicate<Throwable> predicate)
  {
    predicates.add(predicate);
  }

  public void expectCause(final Class<? extends Throwable> type)
  {
    predicates.add(thrown -> thrown.getCause() != null && type.isInstance(thrown.getCause()));
  }

  public void expectCauseCause(final Class<? extends Throwable> type)
  {
    predicates.add(
        thrown -> thrown.getCause() != null
                  && thrown.getCause().getCause() != null
                  && type.isInstance(thrown.getCause().getCause())
    );
  }

  public void expectMessage(final String message)
  {
    expectedMessage = message;
    messageStartsWith = false;
  }

  public void expectMessageStartsWith(final String message)
  {
    expectedMessage = message;
    messageStartsWith = true;
  }

  @Override
  public void interceptTestMethod(
      final Invocation<Void> invocation,
      final ReflectiveInvocationContext<Method> invocationContext,
      final ExtensionContext extensionContext
  ) throws Throwable
  {
    try {
      invocation.proceed();
    }
    catch (Throwable thrown) {
      verify(thrown);
      return;
    }

    if (expectedType != null || expectedMessage != null) {
      Assertions.fail("Expected test to throw an exception");
    }
  }

  private void verify(final Throwable thrown)
  {
    if (expectedType != null) {
      Assertions.assertInstanceOf(expectedType, thrown);
    }
    if (expectedMessage != null) {
      final String actualMessage = thrown.getMessage();
      if (messageStartsWith) {
        Assertions.assertTrue(actualMessage != null && actualMessage.startsWith(expectedMessage));
      } else {
        Assertions.assertTrue(actualMessage != null && actualMessage.contains(expectedMessage));
      }
    }
    for (final Predicate<Throwable> predicate : predicates) {
      Assertions.assertTrue(predicate.test(thrown));
    }
  }
}
