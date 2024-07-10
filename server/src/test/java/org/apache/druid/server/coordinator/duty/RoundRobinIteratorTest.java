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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableSet;
import junitparams.converters.Nullable;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class RoundRobinIteratorTest
{
  @Test
  public void testIterationOrder()
  {
    final RoundRobinIterator rrIterator = new RoundRobinIterator()
    {
      @Override
      int generateRandomCursorPosition(final int maxBound)
      {
        return 1;
      }
    };
    rrIterator.updateCandidates(ImmutableSet.of("a", "b", "c"));

    final Iterator<String> it = rrIterator.getIterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("b", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("c", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("a", it.next());

    rrIterator.updateCandidates(ImmutableSet.of("x", "y"));

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("y", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("x", it.next());
  }

  @Test
  public void testNoConsecutiveDuplicates()
  {
    final RoundRobinIterator rrIterator = new RoundRobinIterator()
    {
      @Override
      int generateRandomCursorPosition(final int maxBound)
      {
        return 0;
      }
    };
    rrIterator.updateCandidates(ImmutableSet.of("a"));

    final Iterator<String> it = rrIterator.getIterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("a", it.next());

    rrIterator.updateCandidates(ImmutableSet.of("a", "b"));

    // next() should skip the duplicate "a" as it was the last value served
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("b", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("a", it.next());

    rrIterator.updateCandidates(ImmutableSet.of("b", "c"));

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("b", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("c", it.next());
  }

  @Test
  public void testMultipleUpdatesWithSameCandidates()
  {
    final Set<String> input = ImmutableSet.of("1", "2", "3");
    final RoundRobinIterator rrIterator = new RoundRobinIterator()
    {
      @Override
      int generateRandomCursorPosition(final int maxBound)
      {
        return 0;
      }
    };
    rrIterator.updateCandidates(input);
    final Iterator<String> it = rrIterator.getIterator();

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("1", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("2", it.next());

    rrIterator.updateCandidates(input);

    // The cursor should just resume from the last position instead of resetting to 0.
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("3", it.next());

    Assert.assertTrue(it.hasNext());
    Assert.assertEquals("1", it.next());
  }

  @Test
  public void testEmptyIterator()
  {
    final RoundRobinIterator rrIterator = new RoundRobinIterator();
    final Iterator<String> it = rrIterator.getIterator();

    Assert.assertFalse(it.hasNext());
    Assert.assertThrows(NoSuchElementException.class, it::next);
  }

  @Test
  public void testUpdateWithNullCandidate()
  {
    final RoundRobinIterator rrIterator = new RoundRobinIterator();
    Assert.assertThrows(DruidException.class, () -> rrIterator.updateCandidates(null));
  }

  /**
   * Similar to the above tests but with the built-in randomness.
   */
  @Test
  public void testMultipleUpdatesWithRandomCursorPositions()
  {
    final RoundRobinIterator rrIterator = new RoundRobinIterator();

    Set<String> input = ImmutableSet.of("a", "b", "c");
    rrIterator.updateCandidates(input);

    List<String> expectedCandidates = getexpectedCandidates(input, rrIterator.getCurrentCursorPosition(), null);
    List<String> actualCandidates = getObservedCandidates(rrIterator.getIterator(), input.size());
    Assert.assertEquals(expectedCandidates, actualCandidates);
    String lastValue = actualCandidates.get(actualCandidates.size() - 1);

    // Second update
    input = ImmutableSet.of("c", "d");
    rrIterator.updateCandidates(input);

    expectedCandidates = getexpectedCandidates(input, rrIterator.getCurrentCursorPosition(), lastValue);
    actualCandidates = getObservedCandidates(rrIterator.getIterator(), input.size());
    Assert.assertEquals(expectedCandidates, actualCandidates);

    Assert.assertNotEquals(lastValue, actualCandidates.get(0));
    lastValue = actualCandidates.get(actualCandidates.size() - 1);

    // Third update
    input = ImmutableSet.of("d", "e");
    rrIterator.updateCandidates(ImmutableSet.of("d", "e"));

    expectedCandidates = getexpectedCandidates(input, rrIterator.getCurrentCursorPosition(), lastValue);
    actualCandidates = getObservedCandidates(rrIterator.getIterator(), input.size());
    Assert.assertEquals(expectedCandidates, actualCandidates);

    Assert.assertNotEquals(lastValue, actualCandidates.get(0));
    lastValue = actualCandidates.get(actualCandidates.size() - 1);

    // Fourth update
    input = ImmutableSet.of("e", "f", "h");
    rrIterator.updateCandidates(input);

    expectedCandidates = getexpectedCandidates(input, rrIterator.getCurrentCursorPosition(), lastValue);
    actualCandidates = getObservedCandidates(rrIterator.getIterator(), input.size());
    Assert.assertEquals(expectedCandidates, actualCandidates);
    Assert.assertNotEquals(lastValue, actualCandidates.get(0));
  }

  private List<String> getexpectedCandidates(
      final Set<String> input,
      final int cursorPosition,
      @Nullable final String previousValue
  )
  {
    final List<String> sortedCandidates = input.stream().sorted().collect(Collectors.toList());
    final List<String> expectedCandidates = new ArrayList<>();

    // Adjust the cursor position if the value at the cursor position from the sorted list
    // is the same as the previous value.
    final int n = sortedCandidates.size();
    int adjustedCursorPosition = cursorPosition;
    if (sortedCandidates.get(adjustedCursorPosition % n).equals(previousValue)) {
      adjustedCursorPosition = (adjustedCursorPosition + 1) % n;
    }

    for (int i = 0; i < n; i++) {
      int index = (adjustedCursorPosition + i) % n;
      expectedCandidates.add(sortedCandidates.get(index));
    }
    return expectedCandidates;
  }

  private List<String> getObservedCandidates(final Iterator<String> it, final int maxSize)
  {
    final List<String> observedCandidates = new ArrayList<>();
    int cnt = 0;
    while (it.hasNext()) {
      observedCandidates.add(it.next());
      if (++cnt >= maxSize) {
        break;
      }
    }
    return observedCandidates;
  }
}
