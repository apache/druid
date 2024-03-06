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

public class DruidExceptionMatcher extends DiagnosingMatcher<Throwable>
{
  public static DruidExceptionMatcher invalidInput()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.USER,
        DruidException.Category.INVALID_INPUT,
        "invalidInput"
    );
  }

  public static DruidExceptionMatcher invalidSqlInput()
  {
    return invalidInput().expectContext("sourceType", "sql");
  }

  public static DruidExceptionMatcher defensive()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.DEVELOPER,
        DruidException.Category.DEFENSIVE,
        "general"
    );
  }

  private final AllOf<DruidException> delegate;
  private final ArrayList<Matcher<? super DruidException>> matcherList;

  public DruidExceptionMatcher(
      DruidException.Persona targetPersona,
      DruidException.Category category,
      String errorCode
  )
  {
    matcherList = new ArrayList<>();
    matcherList.add(DruidMatchers.fn("targetPersona", DruidException::getTargetPersona, Matchers.is(targetPersona)));
    matcherList.add(DruidMatchers.fn("category", DruidException::getCategory, Matchers.is(category)));
    matcherList.add(DruidMatchers.fn("errorCode", DruidException::getErrorCode, Matchers.is(errorCode)));

    delegate = new AllOf<>(matcherList);
  }

  public DruidExceptionMatcher expectContext(String key, String value)
  {
    matcherList.add(0, DruidMatchers.fn("context", DruidException::getContext, Matchers.hasEntry(key, value)));
    return this;
  }

  public DruidExceptionMatcher expectMessageIs(String s)
  {
    return expectMessage(Matchers.equalTo(s));
  }

  public DruidExceptionMatcher expectMessageContains(String contains)
  {
    return expectMessage(Matchers.containsString(contains));
  }

  public DruidExceptionMatcher expectMessage(Matcher<String> messageMatcher)
  {
    matcherList.add(0, DruidMatchers.fn("message", DruidException::getMessage, messageMatcher));
    return this;
  }

  public DruidExceptionMatcher expectException(Matcher<Throwable> causeMatcher)
  {
    matcherList.add(0, DruidMatchers.fn("cause", DruidException::getCause, causeMatcher));
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
      if (e instanceof DruidException) {
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
    void get();
  }
}
