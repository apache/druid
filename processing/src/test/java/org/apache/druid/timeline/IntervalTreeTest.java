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
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class IntervalTreeTest
{

  @Test
  public void testSize()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    Assert.assertEquals("Size", 6, tree.size());
  }

  @Test
  public void testMatch()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));

    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("Match", "v4", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")));
  }

  @Test
  public void testNoMatch()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-07T00:00:00/P1D"));

    Assert.assertEquals(0, entries.size());
  }


  @Test
  public void testOverlap()
  {
    IntervalTree<String> tree = setupOverlapIntervalTree();
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-02T09:00:00/PT1H"));

    Assert.assertEquals(2, entries.size());
    Assert.assertEquals("Day match", "v2", entries.get(Intervals.of("2025-01-02T00:00:00/P1D")));
    Assert.assertEquals("Year match", "v7", entries.get(Intervals.of("2025-01-01T00:00:00/P1Y")));
  }

  @Test
  public void testSparseOverlap()
  {
    IntervalTree<String> tree = setupSparseOverlapTree();
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-06-03T00:00:00/P1D"));

    Assert.assertEquals(3, entries.size());
    Assert.assertEquals("Match 1", "v5", entries.get(Intervals.of("2025-06-03T00:00:00/P1D")));
    Assert.assertEquals("Match 2", "v9", entries.get(Intervals.of("2025-05-10T00:00:00/P1M")));
    Assert.assertEquals("Match 3", "v11", entries.get(Intervals.of("2025-06-01T00:00:00/P1M")));
  }

  @Test
  public void testRemove()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    tree.remove(Intervals.of("2025-01-02T00:00:00/P1D"));
    Assert.assertEquals("Size", 5, tree.size());
  }

  @Test
  public void testRemoveRootAndMatch()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    tree.remove(Intervals.of("2025-01-03T00:00:00/P1D"));
    Assert.assertEquals("Size", 5, tree.size());
    Map<Interval, String> entries = tree.findEncompassing(Intervals.of("2025-01-04T00:00:00/P1D"));
    Assert.assertEquals(1, entries.size());
    Assert.assertEquals("Match", "v4", entries.get(Intervals.of("2025-01-04T00:00:00/P1D")));
  }

  @Test
  public void testRemoveMultiple()
  {
    IntervalTree<String> tree = setupSparseOverlapTree();
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
    IntervalTree<String> tree = setupBaseIntervalTree();
    tree.clear();
    Assert.assertEquals("Size", 0, tree.size());
  }

  /*
  @Test
  public void testTree()
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStartThenEnd(), Comparators.intervalsByEndThenStart());
    Map<Interval, String> data = new HashMap<>();
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      //int month = random.nextInt(12) + 1;
      //int day = random.nextInt(28) + 1;
      int month = 1;
      int day = i + 1;
      String timestr = "2024-" + month + "-" + day + "T00:00:00/P" + (i + 1) + "D";
      Interval interval = Intervals.of(timestr);
      String value = "v" + i;
      tree.add(interval, value);
      data.put(interval, value);
    }
    for (Map.Entry<Interval, String> entry : data.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
        /--*
        List<IntervalTree.Entry<String>> intervals = tree.findEncompassing(Intervals.of("2024-01-04T10:00:00/PT1H"));
        System.out.println("Matched intervals");
        for (IntervalTree.Entry<String> entry : intervals) {
            System.out.println(entry.interval + ": " + entry.value);
        }
        *--/
  }
  */

  @Test
  public void testRebalance() throws JsonProcessingException
  {
    IntervalTree<String> tree = setupSparseOverlapTree(60);
    System.out.println(tree.height());
    tree.rebalance();
    System.out.println(tree.height());
    System.out.println(tree.print());
  }

  private IntervalTree<String> setupBaseIntervalTree()
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    tree.add(Intervals.of("2025-01-03T00:00:00/P1D"), "v3");
    tree.add(Intervals.of("2025-01-01T00:00:00/P1D"), "v1");
    tree.add(Intervals.of("2025-01-02T00:00:00/P1D"), "v2");
    tree.add(Intervals.of("2025-01-04T00:00:00/P1D"), "v4");
    tree.add(Intervals.of("2025-01-05T00:00:00/P1D"), "v5");
    tree.add(Intervals.of("2025-01-06T00:00:00/P1D"), "v6");

    return tree;
  }

  private IntervalTree<String> setupOverlapIntervalTree()
  {
    IntervalTree<String> tree = setupBaseIntervalTree();
    tree.add(Intervals.of("2025-01-01T00:00:00/P1Y"), "v7");

    return tree;
  }

  private IntervalTree<String> setupSparseOverlapTree()
  {
    return setupSparseOverlapTree(null);
  }

  private IntervalTree<String> setupSparseOverlapTree(Integer imbalanceTolerance)
  {
    IntervalTree<String> tree = new IntervalTree<>(Comparators.intervalsByStart(), Comparators.intervalsByEnd());
    if (imbalanceTolerance != null) {
      tree.setImbalanceTolerance(imbalanceTolerance);
    }
    tree.add(Intervals.of("2025-01-01T00:00:00/P1D"), "v1");
    tree.add(Intervals.of("2025-02-01T00:00:00/P1D"), "v2");
    tree.add(Intervals.of("2025-01-12T00:00:00/P1D"), "v3");
    tree.add(Intervals.of("2025-07-12T00:00:00/P1D"), "v4");
    tree.add(Intervals.of("2025-06-03T00:00:00/P1D"), "v5");
    tree.add(Intervals.of("2025-08-09T00:00:00/P1D"), "v6");
    tree.add(Intervals.of("2025-09-04T00:00:00/P1D"), "v7");
    tree.add(Intervals.of("2025-04-02T00:00:00/P1D"), "v8");
    tree.add(Intervals.of("2025-05-10T00:00:00/P1M"), "v9");
    tree.add(Intervals.of("2025-10-06T00:00:00/P1M"), "v10");
    tree.add(Intervals.of("2025-06-01T00:00:00/P1M"), "v11");

    return tree;
  }

}
