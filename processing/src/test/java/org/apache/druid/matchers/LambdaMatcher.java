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

package org.apache.druid.matchers;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import java.util.function.Function;

public class LambdaMatcher<T, S> extends DiagnosingMatcher<T>
{
  private final String name;
  private final Function<T, S> fn;
  private final Matcher<S> matcher;

  public LambdaMatcher(
      String name,
      Function<T, S> fn,
      Matcher<S> matcher
  )
  {
    this.name = name;
    this.fn = fn;
    this.matcher = matcher;
  }

  @Override
  protected boolean matches(Object item, Description mismatchDescription)
  {
    final S result = fn.apply((T) item);
    if (!matcher.matches(result)) {
      matcher.describeMismatch(result, mismatchDescription);
      return false;
    }
    return true;
  }

  @Override
  public void describeTo(Description description)
  {
    description.appendText(name);
    matcher.describeTo(description);
  }
}
