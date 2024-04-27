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

import org.apache.druid.java.util.common.IAE;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;

public class DruidMatchers
{
  public static <T, S> LambdaMatcher<T, S> fn(String name, Function<T, S> fn, Matcher<S> matcher)
  {
    return new LambdaMatcher<>(name + ": ", fn, matcher);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <K, V> Matcher<Map<? extends K, ? extends V>> mapMatcher(Object... keysAndValues)
  {
    if (keysAndValues.length % 2 == 1) {
      throw new IAE("keysAndValues should be pairs, but had an odd length [%s]", keysAndValues.length);
    }
    ArrayList<Matcher<Map<? extends K, ? extends V>>> entryMatchers = new ArrayList<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      entryMatchers.add(Matchers.hasEntry((K) keysAndValues[i], (V) keysAndValues[i + 1]));
    }
    return Matchers.allOf((Iterable) entryMatchers);
  }
}
