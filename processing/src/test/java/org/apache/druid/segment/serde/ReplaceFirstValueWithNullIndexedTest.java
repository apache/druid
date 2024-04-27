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

package org.apache.druid.segment.serde;

import com.google.common.collect.Lists;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Collections;

/**
 * Test for {@link ReplaceFirstValueWithNullIndexed}.
 */
public class ReplaceFirstValueWithNullIndexedTest extends InitializedNullHandlingTest
{
  @Test
  public void testSizeZero()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> new ReplaceFirstValueWithNullIndexed<>(Indexed.empty())
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Size[0] must be >= 1"))
    );
  }

  @Test
  public void testSizeOne()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(new ListIndexed<>("bar"));

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(1, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertFalse(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assert.assertEquals(Collections.singletonList(null), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeTwo()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(new ListIndexed<>("bar", "foo"));

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(1, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(2, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertEquals("foo", indexed.get(1));
    Assert.assertFalse(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assert.assertEquals(Lists.newArrayList(null, "foo"), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeOneSorted()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(
            GenericIndexed.fromArray(
                new String[]{"bar"},
                GenericIndexed.STRING_STRATEGY
            )
        );

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(1, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertTrue(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assert.assertEquals(Collections.singletonList(null), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeTwoSorted()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(
            GenericIndexed.fromArray(
                new String[]{"bar", "foo"},
                GenericIndexed.STRING_STRATEGY
            )
        );

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(1, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-3, indexed.indexOf("qux"));
    Assert.assertEquals(2, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertEquals("foo", indexed.get(1));
    Assert.assertTrue(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assert.assertEquals(Lists.newArrayList(null, "foo"), Lists.newArrayList(indexed));
  }
}
