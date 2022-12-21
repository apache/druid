package org.apache.druid.testsEx.utils;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class RegexMatchUtil
{
  public static Matcher<String> matchesRegex(final String regex) {
    return new TypeSafeMatcher<String>() {
      @Override
      public void describeTo(Description description)
      {
        description.appendText(". The exception message did not match with the regex provided");
      }

      @Override
      protected boolean matchesSafely(final String item) {
        return item.matches(regex);
      }
    };
  }
}
