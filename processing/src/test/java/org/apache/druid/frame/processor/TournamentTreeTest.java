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

package org.apache.druid.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntComparators;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

public class TournamentTreeTest
{
  @Test
  public void test_construction_oneElement()
  {
    final IntComparator intComparator = IntComparators.NATURAL_COMPARATOR;
    final TournamentTree tree = new TournamentTree(1, intComparator);

    Assert.assertEquals(0, tree.getMin());
    Assert.assertArrayEquals(
        "construction",
        new int[]{0},
        tree.backingArray()
    );
  }

  @Test
  public void test_construction_tenElements_natural()
  {
    final IntComparator intComparator = IntComparators.NATURAL_COMPARATOR;
    final TournamentTree tree = new TournamentTree(10, intComparator);

    Assert.assertEquals(0, tree.getMin());
    Assert.assertArrayEquals(
        "construction",
        new int[]{0, 8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13, 15},
        tree.backingArray()
    );
  }

  @Test
  public void test_construction_tenElements_reverse()
  {
    final IntComparator intComparator = IntComparators.OPPOSITE_COMPARATOR;
    final TournamentTree tree = new TournamentTree(10, intComparator);

    Assert.assertEquals(9, tree.getMin());
    Assert.assertArrayEquals(
        "construction",
        new int[]{9, 7, 3, 12, 1, 5, 10, 14, 0, 2, 4, 6, 8, 11, 13, 15},
        tree.backingArray()
    );
  }

  @Test
  public void test_construction_sixteenElements_reverse()
  {
    final IntComparator intComparator = IntComparators.OPPOSITE_COMPARATOR;
    final TournamentTree tree = new TournamentTree(16, intComparator);

    Assert.assertEquals(15, tree.getMin());
    Assert.assertArrayEquals(
        "construction",
        new int[]{15, 7, 3, 11, 1, 5, 9, 13, 0, 2, 4, 6, 8, 10, 12, 14},
        tree.backingArray()
    );
  }

  @Test
  public void test_merge_eightLists()
  {
    final List<List<Integer>> lists = ImmutableList.of(
        ImmutableList.of(0, 1, 1, 5),
        ImmutableList.of(0, 4),
        ImmutableList.of(1, 5, 5, 6, 9),
        ImmutableList.of(1, 6, 7, 8),
        ImmutableList.of(2, 2, 3, 5, 7),
        ImmutableList.of(0, 2, 4, 8, 9),
        ImmutableList.of(1, 2, 4, 6, 7, 7),
        ImmutableList.of(1, 3, 6, 7, 7)
    );

    final List<Deque<Integer>> queues = new ArrayList<>();
    for (final List<Integer> list : lists) {
      final Deque<Integer> queue = new ArrayDeque<>();
      queues.add(queue);
      for (int i : list) {
        queue.addLast(i);
      }
    }

    final IntComparator intComparator = (a, b) -> {
      final Integer itemA = queues.get(a).peek();
      final Integer itemB = queues.get(b).peek();
      return Ordering.natural().nullsLast().compare(itemA, itemB);
    };

    final TournamentTree tree = new TournamentTree(lists.size(), intComparator);

    final List<Integer> intsRead = new ArrayList<>();
    while (queues.get(tree.getMin()).peek() != null) {
      intsRead.add(queues.get(tree.getMin()).poll());
    }

    final List<Integer> expected = new ArrayList<>();
    expected.addAll(Arrays.asList(0, 0, 0));
    expected.addAll(Arrays.asList(1, 1, 1, 1, 1, 1));
    expected.addAll(Arrays.asList(2, 2, 2, 2));
    expected.addAll(Arrays.asList(3, 3));
    expected.addAll(Arrays.asList(4, 4, 4));
    expected.addAll(Arrays.asList(5, 5, 5, 5));
    expected.addAll(Arrays.asList(6, 6, 6, 6));
    expected.addAll(Arrays.asList(7, 7, 7, 7, 7, 7));
    expected.addAll(Arrays.asList(8, 8));
    expected.addAll(Arrays.asList(9, 9));

    Assert.assertEquals(expected, intsRead);
  }

  @Test
  public void test_merge_tenLists()
  {
    final List<List<Integer>> lists = ImmutableList.of(
        ImmutableList.of(0, 1, 1, 5),
        ImmutableList.of(0, 4),
        ImmutableList.of(1, 5, 5, 6, 9),
        ImmutableList.of(1, 6, 7, 8),
        ImmutableList.of(2, 2, 3, 5, 7),
        ImmutableList.of(0, 2, 4, 8, 9),
        ImmutableList.of(1, 2, 4, 6, 7, 7),
        ImmutableList.of(1, 3, 6, 7, 7),
        ImmutableList.of(1, 3, 3, 4, 5, 6),
        ImmutableList.of(4, 4, 6, 7)
    );

    final List<Deque<Integer>> queues = new ArrayList<>();
    for (final List<Integer> list : lists) {
      final Deque<Integer> queue = new ArrayDeque<>();
      queues.add(queue);
      for (int i : list) {
        queue.addLast(i);
      }
    }

    final IntComparator intComparator = (a, b) -> {
      final Integer itemA = queues.get(a).peek();
      final Integer itemB = queues.get(b).peek();
      return Ordering.natural().nullsLast().compare(itemA, itemB);
    };

    final TournamentTree tree = new TournamentTree(lists.size(), intComparator);

    final List<Integer> intsRead = new ArrayList<>();
    while (queues.get(tree.getMin()).peek() != null) {
      intsRead.add(queues.get(tree.getMin()).poll());
    }

    final List<Integer> expected = new ArrayList<>();
    expected.addAll(Arrays.asList(0, 0, 0));
    expected.addAll(Arrays.asList(1, 1, 1, 1, 1, 1, 1));
    expected.addAll(Arrays.asList(2, 2, 2, 2));
    expected.addAll(Arrays.asList(3, 3, 3, 3));
    expected.addAll(Arrays.asList(4, 4, 4, 4, 4, 4));
    expected.addAll(Arrays.asList(5, 5, 5, 5, 5));
    expected.addAll(Arrays.asList(6, 6, 6, 6, 6, 6));
    expected.addAll(Arrays.asList(7, 7, 7, 7, 7, 7, 7));
    expected.addAll(Arrays.asList(8, 8));
    expected.addAll(Arrays.asList(9, 9));

    Assert.assertEquals(expected, intsRead);
  }
}
