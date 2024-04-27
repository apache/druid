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
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Collections;
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
    Assert.assertEquals(empty, CollectionUtils.subtract(empty, empty));
    Assert.assertEquals(abc, CollectionUtils.subtract(abc, empty));
    Assert.assertEquals(empty, CollectionUtils.subtract(abc, abc));
    Assert.assertEquals(abc, CollectionUtils.subtract(abc, efg));
    Assert.assertEquals(ImmutableSet.of("a"), CollectionUtils.subtract(abc, bcd));
  }

  @Test
  public void testIntersect()
  {
    Assert.assertEquals(empty, CollectionUtils.intersect(empty, empty));
    Assert.assertEquals(abc, CollectionUtils.intersect(abc, abc));
    Assert.assertEquals(empty, CollectionUtils.intersect(abc, efg));
    Assert.assertEquals(ImmutableSet.of("b", "c"), CollectionUtils.intersect(abc, bcd));
  }

  @Test
  public void testUnion()
  {
    Assert.assertEquals(empty, CollectionUtils.union(empty, empty));
    Assert.assertEquals(abc, CollectionUtils.union(abc, abc));
    Assert.assertEquals(ImmutableSet.of("a", "b", "c", "e", "f", "g"), CollectionUtils.union(abc, efg));
    Assert.assertEquals(ImmutableSet.of("a", "b", "c", "d"), CollectionUtils.union(abc, bcd));
  }

  @Test
  public void testGetOnlyElement_empty()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> CollectionUtils.getOnlyElement(Collections.emptyList(), xs -> new ISE("oops"))
    );
    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("oops")));
  }

  @Test
  public void testGetOnlyElement_one()
  {
    Assert.assertEquals(
        "a",
        CollectionUtils.getOnlyElement(Collections.singletonList("a"), xs -> new ISE("oops"))
    );
  }

  @Test
  public void testGetOnlyElement_two()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> CollectionUtils.getOnlyElement(ImmutableList.of("a", "b"), xs -> new ISE("oops"))
    );
    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("oops")));
  }
}
