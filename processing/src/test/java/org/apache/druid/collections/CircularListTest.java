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

package org.apache.druid.collections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class CircularListTest
{
  @Test
  public void testIterateInNaturalOrder()
  {
    final Set<String> input = ImmutableSet.of("b", "a", "c");
    final CircularList<String> circularList = new CircularList<>(input, Comparator.naturalOrder());
    final List<String> observedElements = new ArrayList<>();
    int cnt = 0;
    for (String x : circularList) {
      observedElements.add(x);
      if (++cnt >= input.size()) {
        break;
      }
    }
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), observedElements);
  }

  @Test
  public void testIterateInReverseOrder()
  {
    final Set<Integer> input = ImmutableSet.of(-1, 100, 0, -4);
    final CircularList<Integer> circularList = new CircularList<>(input, Comparator.reverseOrder());
    final List<Integer> observedElements = new ArrayList<>();
    int cnt = 0;
    for (Integer x : circularList) {
      observedElements.add(x);
      if (++cnt >= 2 * input.size()) {
        break;
      }
    }

    Assert.assertEquals(ImmutableList.of(100, 0, -1, -4, 100, 0, -1, -4), observedElements);
  }

  @Test
  public void testIteratorResumesFromLastPosition()
  {
    final Set<String> input = ImmutableSet.of("a", "b", "c", "d", "e", "f");
    final CircularList<String> circularList = new CircularList<>(input, Comparator.naturalOrder());

    List<String> observedElements = new ArrayList<>();
    int cnt = 0;
    for (String element : circularList) {
      observedElements.add(element);
      if (++cnt >= input.size() / 2) {
        break;
      }
    }

    Assert.assertEquals(ImmutableList.of("a", "b", "c"), observedElements);

    observedElements = new ArrayList<>();
    for (String element : circularList) {
      observedElements.add(element);
      // Resume and go till the end, and add two more elements looping around
      if (++cnt == input.size() + 2) {
        break;
      }
    }

    Assert.assertEquals(ImmutableList.of("d", "e", "f", "a", "b"), observedElements);
  }

  @Test
  public void testEqualsSet()
  {
    final Set<String> input = ImmutableSet.of("a", "b", "c");
    final CircularList<String> circularList = new CircularList<>(input, Comparator.naturalOrder());
    Assert.assertTrue(circularList.equalsSet(ImmutableSet.of("b", "a", "c")));
    Assert.assertFalse(circularList.equalsSet(ImmutableSet.of("c")));
    Assert.assertFalse(circularList.equalsSet(ImmutableSet.of("a", "c")));
  }

  @Test
  public void testEmptyIterator()
  {
    final Set<String> input = ImmutableSet.of();
    final CircularList<String> circularList = new CircularList<>(input, Comparator.naturalOrder());
    final List<String> observedElements = new ArrayList<>();

    int cnt = 0;
    for (String x : circularList) {
      observedElements.add(x);
      if (++cnt >= input.size()) {
        break;
      }
    }
    Assert.assertEquals(ImmutableList.of(), observedElements);
  }

  @Test
  public void testNextOnEmptyIteratorThrowsException()
  {
    final Set<String> input = ImmutableSet.of();
    final CircularList<String> circularList = new CircularList<>(input, Comparator.naturalOrder());

    final Iterator<String> iterator = circularList.iterator();
    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }
}
