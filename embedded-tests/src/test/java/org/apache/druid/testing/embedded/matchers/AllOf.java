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

public class AllOf<T> extends DiagnosingMatcher<T>
{
  private final Iterable<? extends Matcher<? super T>> matchers;

  public AllOf(final Iterable<? extends Matcher<? super T>> matchers)
  {
    this.matchers = matchers;
  }

  public static <T> Matcher<T> allOf(final Iterable<? extends Matcher<? super T>> matchers)
  {
    return new AllOf<>(matchers);
  }

  @Override
  public boolean matches(final Object actual, final Description mismatchDescription)
  {
    for (final Matcher<? super T> matcher : matchers) {
      if (!matcher.matches(actual)) {
        matcher.describeMismatch(actual, mismatchDescription);
        return false;
      }
    }
    return true;
  }

  @Override
  public void describeTo(final Description description)
  {
    description.appendText("all conditions");
  }
}
