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

package org.apache.druid.testing.embedded.matchers;

import org.junit.jupiter.api.Assertions;

public class MatcherAssert
{
  private MatcherAssert()
  {
  }

  public static <T> void assertThat(final T actual, final Matcher<? super T> matcher)
  {
    Assertions.assertTrue(matcher.matches(actual), matcher.describe());
  }

  public static <T> void assertThat(final String reason, final T actual, final Matcher<? super T> matcher)
  {
    Assertions.assertTrue(matcher.matches(actual), reason + ": " + matcher.describe());
  }

  /**
   * Supports matchers supplied by shared test fixtures that have not yet migrated off Hamcrest.
   */
  public static <T> void assertThat(final T actual, final Object matcher)
  {
    try {
      final boolean matches = (boolean) matcher.getClass().getMethod("matches", Object.class).invoke(matcher, actual);
      Assertions.assertTrue(matches, matcher::toString);
    }
    catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Unsupported matcher type: " + matcher.getClass().getName(), e);
    }
  }

  public static <T> void assertThat(final String reason, final T actual, final Object matcher)
  {
    try {
      final boolean matches = (boolean) matcher.getClass().getMethod("matches", Object.class).invoke(matcher, actual);
      Assertions.assertTrue(matches, () -> reason + ": " + matcher);
    }
    catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Unsupported matcher type: " + matcher.getClass().getName(), e);
    }
  }
}
