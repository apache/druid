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

import com.google.common.base.Throwables;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A JUnit 5 assertion helper for validating exceptions by type, message, and cause.
 */
public class ExceptionMatcher
{

  public static ExceptionMatcher of(Class<? extends Throwable> clazz)
  {
    return new ExceptionMatcher(clazz);
  }

  private final List<Consumer<Throwable>> assertions = new ArrayList<>();
  private final Class<? extends Throwable> clazz;

  public ExceptionMatcher(Class<? extends Throwable> clazz)
  {
    this.clazz = clazz;

  }

  public ExceptionMatcher expectMessageIs(final String message)
  {
    assertions.add(exception -> Assertions.assertEquals(message, exception.getMessage()));
    return this;
  }

  public ExceptionMatcher expectMessageContains(final String contains)
  {
    assertions.add(exception -> {
      Assertions.assertNotNull(exception.getMessage());
      Assertions.assertTrue(exception.getMessage().contains(contains));
    });
    return this;
  }

  public ExceptionMatcher expectCause(final ExceptionMatcher causeMatcher)
  {
    assertions.add(exception -> causeMatcher.assertThat(exception.getCause()));
    return this;
  }

  public ExceptionMatcher expectRootCause(final ExceptionMatcher causeMatcher)
  {
    assertions.add(exception -> causeMatcher.assertThat(Throwables.getRootCause(exception)));
    return this;
  }

  public boolean matches(final Throwable actual)
  {
    try {
      assertThat(actual);
      return true;
    }
    catch (AssertionError e) {
      return false;
    }
  }

  public void assertThat(final Throwable actual)
  {
    Assertions.assertNotNull(actual);
    assertions.forEach(assertion -> assertion.accept(actual));
  }

  public void assertThrowsAndMatches(final ThrowingSupplier fn)
  {
    final Throwable exception = Assertions.assertThrows(clazz, fn::get);
    assertThat(exception);
  }

  public interface ThrowingSupplier
  {
    void get() throws Exception;
  }
}
