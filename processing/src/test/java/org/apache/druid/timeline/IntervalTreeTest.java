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

package org.apache.druid.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class IntervalTreeTest
{

  @Test
  public void testSize()
  {
    IntervalTree<String> tree = setupTree(baseData);
    Assert.assertEquals("Size", 6, tree.size());
  }

  @Test
  public void testAdd()
  {
    IntervalTree<String> tree = setupTree(baseData);
    compareData(baseData, tree);
  }

  @Test
  public void testMatch()
  {
    IntervalTree<String> tree = setupTree(baseData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));

    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("Match", "v5", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")));
  }

  @Test
  public void testNoMatch()
  {
    IntervalTree<String> tree = setupTree(baseData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-07T00:00:00/P1D"));

    Assert.assertEquals(0, entries.size());
  }

  @Test
  public void testOverlap()
  {
    IntervalTree<String> tree = setupTree(overlapData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-02T09:00:00/PT1H"));

    Assert.assertEquals(2, entries.size());
    Assert.assertEquals("Day match", "v4", entries.get(Intervals.of("2025-01-02T00:00:00/P1D")));
    Assert.assertEquals("Year match", "v7", entries.get(Intervals.of("2025-01-01T00:00:00/P1Y")));
  }

  @Test
  public void testSparseOverlap()
  {
    IntervalTree<String> tree = setupTree(sparseOverlapData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-06-03T00:00:00/P1D"));

    Assert.assertEquals(3, entries.size());
    Assert.assertEquals("Match 1", "v5", entries.get(Intervals.of("2025-06-03T00:00:00/P1D")));
    Assert.assertEquals("Match 2", "v9", entries.get(Intervals.of("2025-05-10T00:00:00/P1M")));
    Assert.assertEquals("Match 3", "v11", entries.get(Intervals.of("2025-06-01T00:00:00/P1M")));
  }

  @Test
  public void testRemove()
  {
    IntervalTree<String> tree = setupTree(sparseOverlapData);
    int size = tree.size();

    // Remove leaf
    String intervalstr = "2025-06-01T00:00:00/P1M";
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of(intervalstr));
    Assert.assertEquals("Value size", 1, entries.size());
    String value = entries.values().iterator().next();

    tree.remove(Intervals.of(intervalstr));
    size--;
    Assert.assertEquals("Size", size, tree.size());
    List<Pair<Interval, String>> expectedData = new ArrayList<>(sparseOverlapData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);

    // Remove node in penultimate level
    intervalstr = "2025-09-04T00:00:00/P1D";
    entries = tree.findEncompassing(Intervals.of(intervalstr));
    Assert.assertEquals("Value size", 1, entries.size());
    value = entries.values().iterator().next();

    tree.remove(Intervals.of(intervalstr));
    size--;
    Assert.assertEquals("Size", size, tree.size());
    expectedData = new ArrayList<>(expectedData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);

    // Remove node at a higher level
    intervalstr = "2025-07-12T00:00:00/P1D";
    entries = tree.findEncompassing(Intervals.of(intervalstr));
    Assert.assertEquals("Value size", 1, entries.size());
    value = entries.values().iterator().next();

    tree.remove(Intervals.of(intervalstr));
    size--;
    Assert.assertEquals("Size", size, tree.size());
    expectedData = new ArrayList<>(expectedData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);
  }

  @Test
  public void testRemoveRootAndMatch()
  {
    IntervalTree<String> tree = setupTree(baseData);
    tree.remove(Intervals.of("2025-01-03T00:00:00/P1D"));
    Assert.assertEquals("Size", 5, tree.size());
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));
    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("Match", "v5", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")));
  }

  @Test
  public void testRemoveMultiple()
  {
    IntervalTree<String> tree = setupTree(sparseOverlapData);
    int isize = tree.size();
    tree.remove(Intervals.of("2025-01-12T00:00:00/P1D"));
    tree.remove(Intervals.of("2025-06-03T00:00:00/P1D"));
    tree.remove(Intervals.of("2025-06-01T00:00:00/P1M"));
    int csize = tree.size();
    Assert.assertEquals("Size", 3, isize - csize);
  }

  @Test
  public void testClear()
  {
    IntervalTree<String> tree = setupTree(baseData);
    tree.clear();
    Assert.assertEquals("Size", 0, tree.size());
  }

  @Test
  public void testLargeLoadTree()
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    List<Pair<Interval, String>> expectedData = new ArrayList<>();
    Set<String> existingIntervals = new HashSet<>();
    Random random = ThreadLocalRandom.current();
    int total = 100000;
    int count = 0;
    while (count < total) {
      int year = random.nextInt(26) + 2000;
      int month = random.nextInt(12) + 1;
      int day = random.nextInt(28) + 1;
      int hour = random.nextInt(23) + 1;
      String intervalstr = year + "-" + month + "-" + day + "T" + hour + ":00:00/P" + ((count % 30) + 1) + "D";
      if (!existingIntervals.contains(intervalstr)) {
        Interval interval = Intervals.of(intervalstr);
        String value = "v" + count;
        tree.add(interval, value);
        expectedData.add(Pair.of(interval, value));
        existingIntervals.add(intervalstr);
        ++count;
      }
    }
    Assert.assertEquals("Size", total, tree.size());
    compareData(expectedData, tree);
  }

  @Ignore
  @Test
  public void testPerf()
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    List<Pair<Interval, String>> expectedData = new ArrayList<>();
    Map<Interval, String> mappedData = new HashMap<>();
    Set<String> existingIntervals = new HashSet<>();
    Random random = ThreadLocalRandom.current();
    int total = 10000;
    int count = 0;
    while (count < total) {
      int year = random.nextInt(26) + 2000;
      int month = random.nextInt(12) + 1;
      int day = random.nextInt(28) + 1;
      int hour = random.nextInt(23) + 1;
      String intervalstr = year + "-" + month + "-" + day + "T" + hour + ":00:00/P" + ((count % 30) + 1) + "D";
      if (!existingIntervals.contains(intervalstr)) {
        Interval interval = Intervals.of(intervalstr);
        String value = "v" + count;
        tree.add(interval, value);
        mappedData.put(interval, value);
        expectedData.add(Pair.of(interval, value));
        existingIntervals.add(intervalstr);
        ++count;
      }
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < total; i++) {
      Pair<Interval, String> pair = expectedData.get(i);
      Interval interval = pair.lhs;
      for (Map.Entry<Interval, String> entry : mappedData.entrySet()) {
        if (entry.getKey().contains(interval)) {
          break;
        }
      }
    }
    System.out.println("Seq find time " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    for (int i = 0; i < total; i++) {
      Pair<Interval, String> pair = expectedData.get(i);
      Interval interval = pair.lhs;
      tree.findEncompassing(interval);
    }
    System.out.println("Tree find time " + (System.currentTimeMillis() - start));
  }

  @Test
  public void testAutoRebalance() throws JsonProcessingException
  {
    IntervalTree<String> tree = setupTree(sparseOverlapData);
    Assert.assertEquals("Height", 4, tree.height());
    compareData(sparseOverlapData, tree);
  }

  @Test
  public void testManualRebalance() throws JsonProcessingException
  {
    // Set a high threshold so auto-rebalance does not happen
    IntervalTree<String> tree = setupTree(sparseOverlapData, t -> t.setImbalanceTolerance(100));
    Assert.assertEquals("Height", 6, tree.height());
    compareData(sparseOverlapData, tree);
    tree.rebalance();
    Assert.assertEquals("Height", 3, tree.height());
    compareData(sparseOverlapData, tree);
  }

  private void compareData(List<Pair<Interval, String>> inputData, IntervalTree<String> tree)
  {
    Iterator<IntervalTree.Entry<String>> iterator = tree.inOrderTraverse();

    List<Pair<Interval, String>> expected = inputData.stream()
            .sorted((p1, p2) -> Comparators.intervalsByStart().compare(p1.lhs, p2.lhs))
            .collect(Collectors.toList());

    compareEntries(expected.iterator(), iterator);
  }

  private void compareEntries(Iterator<Pair<Interval, String>> expected, Iterator<IntervalTree.Entry<String>> actual)
  {
    while (actual.hasNext()) {
      Assert.assertTrue("Entry available", expected.hasNext());
      Pair<Interval, String> expectedEntry = expected.next();
      IntervalTree.Entry<String> actualEntry = actual.next();
      Assert.assertEquals("Interval match", expectedEntry.lhs, actualEntry.interval);
      Assert.assertEquals("Value match", expectedEntry.rhs, actualEntry.value);
    }
    Assert.assertFalse("Matched all entries", expected.hasNext());
  }

  static List<Pair<Interval, String>> baseData = new ArrayList<>();
  static List<Pair<Interval, String>> overlapData = new ArrayList<>();
  static List<Pair<Interval, String>> sparseOverlapData = new ArrayList<>();

  static {

    baseData.add(Pair.of(Intervals.of("2025-01-03T00:00:00/P1D"), "v1"));
    baseData.add(Pair.of(Intervals.of("2025-01-05T00:00:00/P1D"), "v2"));
    baseData.add(Pair.of(Intervals.of("2025-01-01T00:00:00/P1D"), "v3"));
    baseData.add(Pair.of(Intervals.of("2025-01-02T00:00:00/P1D"), "v4"));
    baseData.add(Pair.of(Intervals.of("2025-01-04T00:00:00/P1D"), "v5"));
    baseData.add(Pair.of(Intervals.of("2025-01-06T00:00:00/P1D"), "v6"));

    overlapData.addAll(baseData);
    overlapData.add(Pair.of(Intervals.of("2025-01-01T00:00:00/P1Y"), "v7"));

    sparseOverlapData.add(Pair.of(Intervals.of("2025-01-01T00:00:00/P1D"), "v1"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-02-01T00:00:00/P1D"), "v2"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-01-12T00:00:00/P1D"), "v3"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-07-12T00:00:00/P1D"), "v4"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-06-03T00:00:00/P1D"), "v5"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-08-09T00:00:00/P1D"), "v6"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-09-04T00:00:00/P1D"), "v7"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-04-02T00:00:00/P1D"), "v8"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-05-10T00:00:00/P1M"), "v9"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-10-06T00:00:00/P1M"), "v10"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-06-01T00:00:00/P1M"), "v11"));
  }

  private IntervalTree<String> setupTree(List<Pair<Interval, String>> inputData)
  {
    return setupTree(inputData, null);
  }

  private IntervalTree<String> setupTree(List<Pair<Interval, String>> inputData, Consumer<IntervalTree<String>> setupFunc)
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    if (setupFunc != null) {
      setupFunc.accept(tree);
    }
    for (Pair<Interval, String> entry : inputData) {
      tree.add(entry.lhs, entry.rhs);
    }
    return tree;
  }

}
