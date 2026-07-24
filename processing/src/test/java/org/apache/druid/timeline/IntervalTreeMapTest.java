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

import org.apache.commons.collections.CollectionUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
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

public class IntervalTreeMapTest
{

  @Test
  public void testSize()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    Assertions.assertEquals(6, tree.size(), "Size");
  }

  @Test
  public void testPut()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    compareData(baseData, tree);
  }

  @Test
  public void testReplace()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    Pair<Interval, String> entry = baseData.get(2);
    Interval interval = entry.lhs;
    String value = entry.rhs;
    String newValue = value + "n";
    String oldValue = tree.put(interval, newValue);
    Assertions.assertEquals(oldValue, value, "Old value match");
  }

  @Test
  public void testGet()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    baseData.forEach(
        (Pair<Interval, String> item) -> {
          Interval interval = item.lhs;
          String evalue = item.rhs;
          String value = tree.get(interval);
          Assertions.assertEquals(evalue, value, "value");
        }
    );
  }

  @Test
  public void testValues()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    Collection<String> values = tree.values();
    Collection<String> bvalues = baseData.stream().map(entry -> entry.rhs).collect(Collectors.toList());
    Assertions.assertTrue(CollectionUtils.isEqualCollection(bvalues, values), "values");
  }

  @Test
  public void testMatch()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));

    Assertions.assertEquals(1, entries.size());
    Assertions.assertEquals("v5", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")), "Match");
  }

  @Test
  public void testNoMatch()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-07T00:00:00/P1D"));

    Assertions.assertEquals(0, entries.size());
  }

  @Test
  public void testOverlap()
  {
    IntervalTreeMap<String> tree = setupTree(overlapData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-02T09:00:00/PT1H"));

    Assertions.assertEquals(2, entries.size());
    Assertions.assertEquals("v4", entries.get(Intervals.of("2025-01-02T00:00:00/P1D")), "Day match");
    Assertions.assertEquals("v7", entries.get(Intervals.of("2025-01-01T00:00:00/P1Y")), "Year match");
  }

  @Test
  public void testSparseOverlap()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-06-03T00:00:00/P1D"));

    Assertions.assertEquals(4, entries.size());
    Assertions.assertEquals("v1", entries.get(Intervals.of("2025-05-10T00:00:00/P1M")), "Match 1");
    Assertions.assertEquals("v7", entries.get(Intervals.of("2025-06-03T00:00:00/P1D")), "Match 2");
    Assertions.assertEquals("v13", entries.get(Intervals.of("2025-06-01T00:00:00/P1M")), "Match 3");
    Assertions.assertEquals("v14", entries.get(Intervals.of("2025-01-01T00:00:00/P1Y")), "Match 4");
  }

  @Test
  public void testRemove()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    int size = tree.size();

    // Remove node that does not exist
    String intervalstr = "2025-03-11T00:00:00/P1M";
    String oldValue = tree.remove(Intervals.of(intervalstr));
    Assertions.assertEquals(size, tree.size(), "Size");
    Assertions.assertNull(oldValue, "Old value");
    List<Pair<Interval, String>> expectedData = new ArrayList<>(sparseOverlapData);
    compareData(expectedData, tree);

    // Remove leaf
    intervalstr = "2025-06-01T00:00:00/P1M";
    String value = tree.get(Intervals.of(intervalstr));
    Assertions.assertNotNull(value, "Value");

    oldValue = tree.remove(Intervals.of(intervalstr));
    size--;
    Assertions.assertEquals(size, tree.size(), "Size");
    Assertions.assertEquals(value, oldValue, "Old value");
    expectedData = new ArrayList<>(sparseOverlapData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);

    // Remove node in penultimate level
    intervalstr = "2025-09-04T00:00:00/P1D";
    value = tree.get(Intervals.of(intervalstr));
    Assertions.assertNotNull(value, "Value");

    oldValue = tree.remove(Intervals.of(intervalstr));
    size--;
    Assertions.assertEquals(size, tree.size(), "Size");
    Assertions.assertEquals(value, oldValue, "Old value");
    expectedData = new ArrayList<>(expectedData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);

    // Remove node at a higher level
    intervalstr = "2025-07-12T00:00:00/P1D";
    value = tree.get(Intervals.of(intervalstr));
    Assertions.assertNotNull(value, "Value");

    oldValue = tree.remove(Intervals.of(intervalstr));
    size--;
    Assertions.assertEquals(size, tree.size(), "Size");
    Assertions.assertEquals(value, oldValue, "Old value");
    expectedData = new ArrayList<>(expectedData);
    expectedData.remove(Pair.of(Intervals.of(intervalstr), value));
    compareData(expectedData, tree);
  }

  @Test
  public void testRemoveRootAndMatch()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    tree.remove(Intervals.of("2025-01-03T00:00:00/P1D"));
    Assertions.assertEquals(5, tree.size(), "Size");
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));
    Assertions.assertEquals(1, entries.size());
    Assertions.assertEquals("v5", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")), "Match");
  }

  @Test
  public void testRemoveMultiple()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    int isize = tree.size();
    tree.remove(Intervals.of("2025-01-12T00:00:00/P1D"));
    tree.remove(Intervals.of("2025-06-03T00:00:00/P1D"));
    tree.remove(Intervals.of("2025-06-01T00:00:00/P1M"));
    int csize = tree.size();
    Assertions.assertEquals(3, isize - csize, "Size");
  }

  @Test
  public void testClear()
  {
    IntervalTreeMap<String> tree = setupTree(baseData);
    tree.clear();
    Assertions.assertEquals(0, tree.size(), "Size");
  }

  @Test
  public void testLargeLoadTree()
  {
    IntervalTreeMap<String> tree = new IntervalTreeMap<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
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
        tree.put(interval, value);
        expectedData.add(Pair.of(interval, value));
        existingIntervals.add(intervalstr);
        ++count;
      }
    }
    Assertions.assertEquals(total, tree.size(), "Size");
    compareData(expectedData, tree);
  }

  private static final Logger log = new Logger(IntervalTreeMapTest.class);

  @Disabled
  @Test
  public void testPerf()
  {
    IntervalTreeMap<String> tree = new IntervalTreeMap<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
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
        tree.put(interval, value);
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
    log.info("Seq find time %d", (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    for (int i = 0; i < total; i++) {
      Pair<Interval, String> pair = expectedData.get(i);
      Interval interval = pair.lhs;
      tree.findEncompassing(interval);
    }
    log.info("Tree find time %d", (System.currentTimeMillis() - start));
  }

  @Test
  public void testAutoRebalance()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    Assertions.assertEquals(4, tree.height(), "Height");
    compareData(sparseOverlapData, tree);
  }

  @Test
  public void testManualRebalance()
  {
    // Set a high threshold so auto-rebalance does not happen
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData, t -> t.setImbalanceTolerance(100));
    Assertions.assertEquals(4, tree.height(), "Height");
    compareData(sparseOverlapData, tree);
    tree.rebalance();
    Assertions.assertEquals(3, tree.height(), "Height");
    compareData(sparseOverlapData, tree);
  }

  @Test
  public void testIsEmpty()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    Assertions.assertFalse(tree.isEmpty(), "Not Empty");
    sparseOverlapData.forEach(t -> tree.remove(t.lhs));
    Assertions.assertTrue(tree.isEmpty(), "Empty");
  }

  @Test
  public void testFirstEntryAndKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    Map.Entry<Interval, String> entry = tree.firstEntry();
    Interval matchInterval = Intervals.of("2025-01-01T00:00:00/P1D");
    Assertions.assertEquals(matchInterval, entry.getKey(), "Entry interval");
    Assertions.assertEquals("v2", entry.getValue(), "Entry value");

    Interval interval = tree.firstKey();
    Assertions.assertEquals(matchInterval, interval, "Interval key");
  }

  @Test
  public void testLastEntryAndKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);
    Map.Entry<Interval, String> entry = tree.lastEntry();
    Interval matchInterval = Intervals.of("2025-10-06T00:00:00/P1M");
    Assertions.assertEquals(matchInterval, entry.getKey(), "Entry interval");
    Assertions.assertEquals("v12", entry.getValue(), "Entry value");

    Interval interval = tree.lastKey();
    Assertions.assertEquals(matchInterval, interval, "Interval key");
  }

  @Test
  public void testFloorKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);

    // Only one smaller entry
    Interval floor = tree.floorKey(Intervals.of("2025-01-11T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1Y"), floor, "Floor key 1");

    // Entry with same start date but different end date
    floor = tree.lowerKey(Intervals.of("2025-01-01T00:00:00/P1M"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1D"), floor, "Lower key 2");

    // Matching entry
    floor = tree.floorKey(Intervals.of("2025-01-12T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-12T00:00:00/P1D"), floor, "Floor key 3");

    // Random entry
    floor = tree.floorKey(Intervals.of("2025-08-01T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-07-12T00:00:00/P1D"), floor, "Floor key 4");

    // Last entry
    floor = tree.floorKey(Intervals.of("2025-11-01T00:00:00/P1M"));
    Assertions.assertEquals(Intervals.of("2025-10-06T00:00:00/P1M"), floor, "Floor key 5");

    // No smaller entry
    floor = tree.floorKey(Intervals.of("2024-12-31T00:00:00/P1D"));
    Assertions.assertNull(floor, "Floor key 6");
  }

  @Test
  public void testLowerKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);

    // Only one smaller entry
    Interval lower = tree.lowerKey(Intervals.of("2025-01-11T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1Y"), lower, "Lower key 1");

    // Entry with same start date but different end date
    lower = tree.lowerKey(Intervals.of("2025-01-01T00:00:00/P1M"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1D"), lower, "Lower key 2");

    // Matching entry
    lower = tree.lowerKey(Intervals.of("2025-01-12T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1Y"), lower, "Lower key 3");

    // Random entry
    lower = tree.lowerKey(Intervals.of("2025-08-01T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-07-12T00:00:00/P1D"), lower, "Lower key 4");

    // Last entry
    lower = tree.lowerKey(Intervals.of("2025-11-01T00:00:00/P1M"));
    Assertions.assertEquals(Intervals.of("2025-10-06T00:00:00/P1M"), lower, "Lower key 5");

    // No smaller entry
    lower = tree.lowerKey(Intervals.of("2024-12-31T00:00:00/P1D"));
    Assertions.assertNull(lower, "Lower key 6");
  }

  @Test
  public void testCeiinglKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);

    // First entry
    Interval ceiling = tree.ceilingKey(Intervals.of("2024-12-31T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1D"), ceiling, "Ceiling key 1");

    // Entry with same start date but different end date
    ceiling = tree.ceilingKey(Intervals.of("2025-02-01T00:00:00/PT6H"));
    Assertions.assertEquals(Intervals.of("2025-02-01T00:00:00/P1D"), ceiling, "Ceiling key 2");

    // Matching entry
    ceiling = tree.ceilingKey(Intervals.of("2025-09-04T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-09-04T00:00:00/P1D"), ceiling, "Ceiling key 3");

    // Random entry
    ceiling = tree.ceilingKey(Intervals.of("2025-03-31T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-04-02T00:00:00/P1D"), ceiling, "Ceiling key 4");

    // Only one greater entry
    ceiling = tree.ceilingKey(Intervals.of("2025-09-28T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-10-06T00:00:00/P1M"), ceiling, "Ceiling key 5");

    // No greater entry
    ceiling = tree.ceilingKey(Intervals.of("2025-11-01T00:00:00/P1D"));
    Assertions.assertNull(ceiling, "Ceiling key 6");
  }

  @Test
  public void testHigherKey()
  {
    IntervalTreeMap<String> tree = setupTree(sparseOverlapData);

    // First entry
    Interval higher = tree.higherKey(Intervals.of("2024-12-31T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-01-01T00:00:00/P1D"), higher, "Higher key 1");

    // Entry with same start date but different end date
    higher = tree.ceilingKey(Intervals.of("2025-02-01T00:00:00/PT6H"));
    Assertions.assertEquals(Intervals.of("2025-02-01T00:00:00/P1D"), higher, "Higher key 2");

    // Matching entry
    higher = tree.higherKey(Intervals.of("2025-09-04T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-10-06T00:00:00/P1M"), higher, "Higher key 3");

    // Random entry
    higher = tree.higherKey(Intervals.of("2025-03-31T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-04-02T00:00:00/P1D"), higher, "Higher key 4");

    // Only one greater entry
    higher = tree.higherKey(Intervals.of("2025-09-28T00:00:00/P1D"));
    Assertions.assertEquals(Intervals.of("2025-10-06T00:00:00/P1M"), higher, "Higher key 5");

    // No greater entry
    higher = tree.higherKey(Intervals.of("2025-11-01T00:00:00/P1D"));
    Assertions.assertNull(higher, "Higher key 6");
  }

  private void compareData(List<Pair<Interval, String>> inputData, IntervalTreeMap<String> tree)
  {
    //Iterator<Map.Entry<Interval, String>> iterator = tree.inOrderTraverse();
    Iterator<Map.Entry<Interval, String>> iterator = tree.entrySet().iterator();

    List<Pair<Interval, String>> expected = inputData.stream()
            .sorted((p1, p2) -> Comparators.intervalsByStartThenEnd().compare(p1.lhs, p2.lhs))
            .collect(Collectors.toList());

    compareEntries(expected.iterator(), iterator);
  }

  private void compareEntries(Iterator<Pair<Interval, String>> expected, Iterator<Map.Entry<Interval, String>> actual)
  {
    while (actual.hasNext()) {
      Assertions.assertTrue(expected.hasNext(), "Entry available");
      Pair<Interval, String> expectedEntry = expected.next();
      Map.Entry<Interval, String> actualEntry = actual.next();
      Assertions.assertEquals(expectedEntry.lhs, actualEntry.getKey(), "Interval match");
      Assertions.assertEquals(expectedEntry.rhs, actualEntry.getValue(), "Value match");
    }
    Assertions.assertFalse(expected.hasNext(), "No outstanding entries");
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

    sparseOverlapData.add(Pair.of(Intervals.of("2025-05-10T00:00:00/P1M"), "v1"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-01-01T00:00:00/P1D"), "v2"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-02-01T00:00:00/P1D"), "v3"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-01-12T00:00:00/P1D"), "v4"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-07-12T00:00:00/P1D"), "v5"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-02-01T00:00:00/P1M"), "v6"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-06-03T00:00:00/P1D"), "v7"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-08-09T00:00:00/P1D"), "v8"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-08-02T00:00:00/P1M"), "v9"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-09-04T00:00:00/P1D"), "v10"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-04-02T00:00:00/P1D"), "v11"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-10-06T00:00:00/P1M"), "v12"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-06-01T00:00:00/P1M"), "v13"));
    sparseOverlapData.add(Pair.of(Intervals.of("2025-01-01T00:00:00/P1Y"), "v14"));

  }

  private IntervalTreeMap<String> setupTree(List<Pair<Interval, String>> inputData)
  {
    return setupTree(inputData, null);
  }

  private IntervalTreeMap<String> setupTree(List<Pair<Interval, String>> inputData, Consumer<IntervalTreeMap<String>> setupFunc)
  {
    IntervalTreeMap<String> tree = new IntervalTreeMap<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    if (setupFunc != null) {
      setupFunc.accept(tree);
    }
    for (Pair<Interval, String> entry : inputData) {
      tree.put(entry.lhs, entry.rhs);
    }
    return tree;
  }

}
