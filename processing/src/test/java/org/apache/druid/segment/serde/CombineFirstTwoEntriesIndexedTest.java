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

import com.google.common.collect.ImmutableList;
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

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * Test for {@link CombineFirstTwoEntriesIndexed}.
 */
public class CombineFirstTwoEntriesIndexedTest extends InitializedNullHandlingTest
{
  @Test
  public void testSizeZero()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> wrap(Indexed.empty(), "xyz")
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Size[0] must be >= 2"))
    );
  }

  @Test
  public void testSizeOne()
  {
    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () -> wrap(new ListIndexed<>("foo"), "xyz")
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Size[1] must be >= 2"))
    );
  }

  @Test
  public void testSizeTwo()
  {
    final CombineFirstTwoEntriesIndexed<String> indexed = wrap(new ListIndexed<>("bar", "foo"), "xyz");
    Assert.assertEquals(0, indexed.indexOf("xyz"));
    Assert.assertEquals(-2, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf(null));
    Assert.assertEquals(1, indexed.size());
    Assert.assertEquals("xyz", indexed.get(0));
    Assert.assertFalse(indexed.isSorted()); // Matches delegate. See class-level note in CombineFirstTwoEntriesIndexed.
    Assert.assertEquals(ImmutableList.of("xyz"), ImmutableList.copyOf(indexed));
  }

  @Test
  public void testSizeThree()
  {
    final CombineFirstTwoEntriesIndexed<String> indexed = wrap(new ListIndexed<>("bar", "baz", "foo"), "xyz");
    Assert.assertEquals(0, indexed.indexOf("xyz"));
    Assert.assertEquals(1, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(-2, indexed.indexOf(null));
    Assert.assertEquals("xyz", indexed.get(0));
    Assert.assertEquals("foo", indexed.get(1));
    Assert.assertFalse(indexed.isSorted()); // Matches delegate. See class-level note in CombineFirstTwoEntriesIndexed.
    Assert.assertEquals(ImmutableList.of("xyz", "foo"), ImmutableList.copyOf(indexed));
  }

  @Test
  public void testSizeTwoSorted()
  {
    final CombineFirstTwoEntriesIndexed<String> indexed = wrap(
        GenericIndexed.fromArray(
            new String[]{"bar", "foo"},
            GenericIndexed.STRING_STRATEGY
        ),
        null
    );

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(-2, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-2, indexed.indexOf("qux"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(1, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertTrue(indexed.isSorted()); // Matches delegate. See class-level note in CombineFirstTwoEntriesIndexed.
    Assert.assertEquals(Collections.singletonList(null), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeThreeSorted()
  {
    final CombineFirstTwoEntriesIndexed<String> indexed = wrap(
        GenericIndexed.fromArray(
            new String[]{"bar", "baz", "foo"},
            GenericIndexed.STRING_STRATEGY
        ),
        null
    );

    Assert.assertEquals(0, indexed.indexOf(null));
    Assert.assertEquals(1, indexed.indexOf("foo"));
    Assert.assertEquals(-2, indexed.indexOf("bar"));
    Assert.assertEquals(-2, indexed.indexOf("baz"));
    Assert.assertEquals(-3, indexed.indexOf("qux"));
    Assert.assertEquals(-2, indexed.indexOf(""));
    Assert.assertEquals(2, indexed.size());
    Assert.assertNull(indexed.get(0));
    Assert.assertEquals("foo", indexed.get(1));
    Assert.assertTrue(indexed.isSorted()); // Matches delegate. See class-level note in CombineFirstTwoEntriesIndexed.
    Assert.assertEquals(Lists.newArrayList(null, "foo"), Lists.newArrayList(indexed));
  }

  private <T> CombineFirstTwoEntriesIndexed<T> wrap(final Indexed<T> indexed, @Nullable final T newFirstValue)
  {
    return new CombineFirstTwoEntriesIndexed<>(indexed)
    {
      @Override
      protected T newFirstValue()
      {
        return newFirstValue;
      }
    };
  }
}
