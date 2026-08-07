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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Matchers
{
  protected Matchers()
  {
  }

  @SafeVarargs
  public static <T> Matcher<T> anyOf(final Matcher<? super T>... matchers)
  {
    return described(value -> Arrays.stream(matchers).anyMatch(matcher -> matcher.matches(value)), "matching any");
  }

  public static <T> Matcher<T> in(final Collection<? extends T> values)
  {
    return described(values::contains, "contained in " + values);
  }

  @SafeVarargs
  public static <T> Matcher<Iterable<? extends T>> containsInAnyOrder(final T... expected)
  {
    return described(actual -> {
      final List<T> remaining = new ArrayList<>(Arrays.asList(expected));
      for (T value : (Iterable<T>) actual) {
        if (!remaining.remove(value)) {
          return false;
        }
      }
      return remaining.isEmpty();
    }, "containing in any order " + Arrays.toString(expected));
  }

  public static <T> Matcher<T> equalTo(final T expected)
  {
    return described(actual -> Objects.deepEquals(expected, actual), "equal to " + expected);
  }

  public static <T> Matcher<T> is(final T expected)
  {
    return equalTo(expected);
  }

  public static <T> Matcher<T> is(final Matcher<T> matcher)
  {
    return matcher;
  }

  public static Matcher<String> containsString(final String expected)
  {
    return described(actual -> actual instanceof String && ((String) actual).contains(expected), "containing " + expected);
  }

  public static Matcher<String> startsWith(final String expected)
  {
    return described(actual -> actual instanceof String && ((String) actual).startsWith(expected), "starting with " + expected);
  }

  public static <T> Matcher<T> instanceOf(final Class<?> expectedClass)
  {
    return described(expectedClass::isInstance, "instance of " + expectedClass.getName());
  }

  public static <T> Matcher<T> not(final Matcher<T> matcher)
  {
    return described(actual -> !matcher.matches(actual), "not " + matcher.describe());
  }

  public static <T extends Comparable<T>> Matcher<T> greaterThan(final T expected)
  {
    return described(actual -> expected.getClass().isInstance(actual) && ((T) actual).compareTo(expected) > 0, "greater than " + expected);
  }

  public static <T extends Comparable<T>> Matcher<T> lessThan(final T expected)
  {
    return described(actual -> expected.getClass().isInstance(actual) && ((T) actual).compareTo(expected) < 0, "less than " + expected);
  }

  public static <T extends Comparable<T>> Matcher<T> greaterThanOrEqualTo(final T expected)
  {
    return described(actual -> expected.getClass().isInstance(actual) && ((T) actual).compareTo(expected) >= 0, "at least " + expected);
  }

  public static <T extends Comparable<T>> Matcher<T> lessThanOrEqualTo(final T expected)
  {
    return described(actual -> expected.getClass().isInstance(actual) && ((T) actual).compareTo(expected) <= 0, "at most " + expected);
  }

  public static <K, V> Matcher<Map<? extends K, ? extends V>> hasEntry(final K key, final V value)
  {
    return described(
        actual -> actual instanceof Map && Objects.equals(((Map<?, ?>) actual).get(key), value),
        "map containing entry " + key + "=" + value
    );
  }

  public static <K> Matcher<Map<? extends K, ?>> hasKey(final K key)
  {
    return described(actual -> actual instanceof Map && ((Map<?, ?>) actual).containsKey(key), "map containing key " + key);
  }

  @SafeVarargs
  public static <T> Matcher<T> allOf(final Matcher<? super T>... matchers)
  {
    return described(
        actual -> {
          for (final Matcher<? super T> matcher : matchers) {
            if (!matcher.matches(actual)) {
              return false;
            }
          }
          return true;
        },
        "all conditions"
    );
  }

  public static <T> Matcher<T> allOf(final Iterable<? extends Matcher<? super T>> matchers)
  {
    return described(
        actual -> {
          for (final Matcher<? super T> matcher : matchers) {
            if (!matcher.matches(actual)) {
              return false;
            }
          }
          return true;
        },
        "all conditions"
    );
  }

  private static <T> Matcher<T> described(final java.util.function.Predicate<Object> predicate, final String description)
  {
    return new Matcher<>()
    {
      @Override
      public boolean matches(final Object actual)
      {
        return predicate.test(actual);
      }

      @Override
      public String describe()
      {
        return description;
      }
    };
  }
}
