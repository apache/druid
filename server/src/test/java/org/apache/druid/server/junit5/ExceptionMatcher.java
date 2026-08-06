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

package org.apache.druid.server.junit5;

import org.junit.jupiter.api.Assertions;

public final class ExceptionMatcher
{
  public static ExceptionMatcher of(Class<? extends Throwable> clazz)
  {
    return new ExceptionMatcher(clazz);
  }

  private final Class<? extends Throwable> clazz;
  private String expectedMessage;
  private boolean messageContains;
  private DruidExceptionMatcher expectedRootCause;

  public ExceptionMatcher(Class<? extends Throwable> clazz)
  {
    this.clazz = clazz;
  }

  public ExceptionMatcher expectMessageIs(String message)
  {
    expectedMessage = message;
    messageContains = false;
    return this;
  }

  public ExceptionMatcher expectMessageContains(String message)
  {
    expectedMessage = message;
    messageContains = true;
    return this;
  }

  public ExceptionMatcher expectRootCause(DruidExceptionMatcher matcher)
  {
    expectedRootCause = matcher;
    return this;
  }

  public boolean matches(Object item)
  {
    if (!(item instanceof Throwable) || !clazz.isInstance(item)) {
      return false;
    }

    final Throwable exception = (Throwable) item;
    final String actualMessage = exception.getMessage();
    if (expectedMessage != null
        && (actualMessage == null
            || (messageContains ? !actualMessage.contains(expectedMessage) : !actualMessage.equals(expectedMessage)))) {
      return false;
    }

    if (expectedRootCause != null) {
      Throwable rootCause = exception;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      if (!expectedRootCause.matches(rootCause)) {
        return false;
      }
    }

    return true;
  }

  public void assertThrowsAndMatches(ThrowingSupplier supplier)
  {
    final Throwable exception = Assertions.assertThrows(Throwable.class, supplier::get);
    Assertions.assertTrue(clazz.isInstance(exception), () -> "Expected " + clazz + " but got " + exception);
    Assertions.assertTrue(matches(exception), () -> "Exception did not match expectations: " + exception);
  }

  public interface ThrowingSupplier
  {
    void get() throws Exception;
  }
}
