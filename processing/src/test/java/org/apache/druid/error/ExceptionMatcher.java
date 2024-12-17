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

import org.apache.druid.matchers.DruidMatchers;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AllOf;

import java.util.ArrayList;

/**
 * A matcher for validating exceptions in unit tests, providing a fluent API for constructing matchers to allow
 * matching of {@link Throwable} objects, such as verifying exception type, message content, and cause.
 */
public class ExceptionMatcher extends DiagnosingMatcher<Throwable>
{

  public static ExceptionMatcher of(Class<? extends Throwable> clazz)
  {
    return new ExceptionMatcher(clazz);
  }

  private final AllOf<Throwable> delegate;
  private final ArrayList<Matcher<? super Throwable>> matcherList;
  private final Class<? extends Throwable> clazz;

  public ExceptionMatcher(Class<? extends Throwable> clazz)
  {
    this.clazz = clazz;

    matcherList = new ArrayList<>();
    delegate = new AllOf<>(matcherList);
  }

  public ExceptionMatcher expectMessageIs(String s)
  {
    return expectMessage(Matchers.equalTo(s));
  }

  public ExceptionMatcher expectMessageContains(String contains)
  {
    return expectMessage(Matchers.containsString(contains));
  }

  public ExceptionMatcher expectMessage(Matcher<String> messageMatcher)
  {
    matcherList.add(0, DruidMatchers.fn("message", Throwable::getMessage, messageMatcher));
    return this;
  }

  public ExceptionMatcher expectCause(Matcher<Throwable> causeMatcher)
  {
    matcherList.add(0, DruidMatchers.fn("cause", Throwable::getCause, causeMatcher));
    return this;
  }

  @Override
  protected boolean matches(Object item, Description mismatchDescription)
  {
    return delegate.matches(item, mismatchDescription);
  }

  @Override
  public void describeTo(Description description)
  {
    delegate.describeTo(description);
  }

  public <T> void assertThrowsAndMatches(ThrowingSupplier fn)
  {
    boolean thrown = false;
    try {
      fn.get();
    }
    catch (Throwable e) {
      if (clazz.isInstance(e)) {
        MatcherAssert.assertThat(e, this);
        thrown = true;
      } else {
        throw new RuntimeException(e);
      }
    }
    MatcherAssert.assertThat(thrown, Matchers.is(true));
  }

  public interface ThrowingSupplier
  {
    void get() throws Exception;
  }
}
