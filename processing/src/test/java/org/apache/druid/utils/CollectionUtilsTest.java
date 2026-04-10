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

package org.apache.druid.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class CollectionUtilsTest
{
  // When Java 9 is allowed, use Set.of().
  Set<String> empty = ImmutableSet.of();
  Set<String> abc = ImmutableSet.of("a", "b", "c");
  Set<String> bcd = ImmutableSet.of("b", "c", "d");
  Set<String> efg = ImmutableSet.of("e", "f", "g");

  @Test
  public void testSubtract()
  {
    Assertions.assertEquals(empty, CollectionUtils.subtract(empty, empty));
    Assertions.assertEquals(abc, CollectionUtils.subtract(abc, empty));
    Assertions.assertEquals(empty, CollectionUtils.subtract(abc, abc));
    Assertions.assertEquals(abc, CollectionUtils.subtract(abc, efg));
    Assertions.assertEquals(ImmutableSet.of("a"), CollectionUtils.subtract(abc, bcd));
  }

  @Test
  public void testIntersect()
  {
    Assertions.assertEquals(empty, CollectionUtils.intersect(empty, empty));
    Assertions.assertEquals(abc, CollectionUtils.intersect(abc, abc));
    Assertions.assertEquals(empty, CollectionUtils.intersect(abc, efg));
    Assertions.assertEquals(ImmutableSet.of("b", "c"), CollectionUtils.intersect(abc, bcd));
  }

  @Test
  public void testUnion()
  {
    Assertions.assertEquals(empty, CollectionUtils.union(empty, empty));
    Assertions.assertEquals(abc, CollectionUtils.union(abc, abc));
    Assertions.assertEquals(ImmutableSet.of("a", "b", "c", "e", "f", "g"), CollectionUtils.union(abc, efg));
    Assertions.assertEquals(ImmutableSet.of("a", "b", "c", "d"), CollectionUtils.union(abc, bcd));
  }

  @Test
  public void testGetOnlyElement_empty()
  {
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> CollectionUtils.getOnlyElement(Collections.emptyList(), xs -> new ISE("oops"))
    );
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.equalTo("oops"));
  }

  @Test
  public void testGetOnlyElement_one()
  {
    Assertions.assertEquals(
        "a",
        CollectionUtils.getOnlyElement(Collections.singletonList("a"), xs -> new ISE("oops"))
    );
  }

  @Test
  public void testGetOnlyElement_two()
  {
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> CollectionUtils.getOnlyElement(ImmutableList.of("a", "b"), xs -> new ISE("oops"))
    );
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.equalTo("oops"));
  }

  @Test
  public void test_toMap()
  {
    Assertions.assertEquals(
        Map.of("a", "A", "b", "B"),
        CollectionUtils.toMap(
            List.of("a", "b"),
            entry -> entry,
            entry -> entry.toUpperCase(Locale.ROOT)
        )
    );
  }
}
