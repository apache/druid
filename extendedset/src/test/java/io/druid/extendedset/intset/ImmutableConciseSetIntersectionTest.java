/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.extendedset.intset;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class ImmutableConciseSetIntersectionTest
{
  @Parameterized.Parameters
  public static List<Object[]> parameters()
  {
    return Arrays.asList(new Object[] {false}, new Object[] {true});
  }

  private boolean compact;

  public ImmutableConciseSetIntersectionTest(boolean compact)
  {
    this.compact = compact;
  }

  /**
   * Testing basic intersection of similar sets
   */
  @Test
  public void testIntersection1()
  {
    final int[] ints1 = {33, 100000};
    final int[] ints2 = {33, 100000};
    List<Integer> expected = Arrays.asList(33, 100000);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  /**
   * Set1: literal, zero fill with flip bit, literal
   * Set2: literal, zero fill with different flip bit, literal
   */
  @Test
  public void testIntersection2()
  {
    final int[] ints1 = {33, 100000};
    final int[] ints2 = {34, 100000};
    List<Integer> expected = Collections.singletonList(100000);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  /**
   * Testing intersection of one fills
   */
  @Test
  public void testIntersection3()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set1.add(i);
      set2.add(i);
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  /**
   * Similar to previous test with one bit in the sequence set to zero
   */
  @Test
  public void testIntersection4()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set1.add(i);
      if (i != 500) {
        set2.add(i);
        expected.add(i);
      }
    }

    verifyIntersection(expected, set1, set2);
  }

  /**
   * Testing with disjoint sets
   */
  @Test
  public void testIntersection5()
  {
    final int[] ints1 = {33, 100000};
    final int[] ints2 = {34, 200000};
    List<Integer> expected = Lists.newArrayList();

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  /**
   * Set 1: literal, zero fill, literal
   * Set 2: one fill, literal that falls within the zero fill above, one fill
   */
  @Test
  public void testIntersection6()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 5; i++) {
      set1.add(i);
    }
    for (int i = 1000; i < 1005; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 800; i < 805; i++) {
      set2.add(i);
    }
    for (int i = 806; i < 1005; i++) {
      set2.add(i);
    }

    for (int i = 1000; i < 1005; i++) {
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection7()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 3100; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    set2.add(100);
    set2.add(500);
    for (int i = 600; i < 700; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    expected.add(100);
    expected.add(500);
    for (int i = 600; i < 700; i++) {
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection8()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 3100; i++) {
      set1.add(i);
    }
    set1.add(4001);

    ConciseSet set2 = new ConciseSet();
    set2.add(100);
    set2.add(500);
    for (int i = 600; i < 700; i++) {
      set2.add(i);
    }
    set2.add(4001);

    List<Integer> expected = Lists.newArrayList();
    expected.add(100);
    expected.add(500);
    for (int i = 600; i < 700; i++) {
      expected.add(i);
    }
    expected.add(4001);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection9()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);
    set1.add(3005);
    set1.add(3008);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 3007; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    expected.add(2005);
    expected.add(3005);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection10()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 3100; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();

    set2.add(500);
    set2.add(600);
    set2.add(4001);

    List<Integer> expected = Lists.newArrayList();
    expected.add(500);
    expected.add(600);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection11()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);
    for (int i = 2800; i < 3500; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 3007; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    expected.add(2005);
    for (int i = 2800; i < 3007; i++) {
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection12()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);
    for (int i = 2800; i < 3500; i++) {
      set1.add(i);
    }
    set1.add(10005);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 3007; i++) {
      set2.add(i);
    }
    set2.add(10005);

    List<Integer> expected = Lists.newArrayList();
    expected.add(2005);
    for (int i = 2800; i < 3007; i++) {
      expected.add(i);
    }
    expected.add(10005);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection13()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 100; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection14()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    set2.add(0);
    set2.add(3);
    set2.add(5);
    set2.add(100);
    set2.add(101);

    List<Integer> expected = Lists.newArrayList();
    expected.add(0);
    expected.add(3);
    expected.add(5);
    expected.add(100);
    expected.add(101);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection15()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    set2.add(0);
    set2.add(3);
    set2.add(5);
    for (int i = 100; i < 500; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    expected.add(0);
    expected.add(3);
    expected.add(5);
    for (int i = 100; i < 500; i++) {
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection16()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);

    ConciseSet set2 = new ConciseSet();
    set2.add(0);
    set2.add(3);
    set2.add(5);
    set2.add(100);
    set2.add(101);

    List<Integer> expected = Lists.newArrayList();

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection17()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 4002; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    set2.add(4001);

    List<Integer> expected = Lists.newArrayList();
    expected.add(4001);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection18()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 32; i < 93; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 62; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    for (int i = 32; i < 62; i++) {
      expected.add(i);
    }

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersection19()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(2005);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 10000; i++) {
      set2.add(i);
    }

    List<Integer> expected = Lists.newArrayList();
    expected.add(2005);

    verifyIntersection(expected, set1, set2);
  }

  @Test
  public void testIntersectionLiteralAndOneFill()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 31; i += 2) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (i != 2) {
        set2.add(i);
      }
    }
    verifyIntersection(set1, set2);
  }

  @Test
  public void testIntersectionZeroSequenceRemovedFromQueue()
  {
    // Seems that it is impossible to test this case with naturally constructed ConciseSet, because the end of the
    // sequence is defined by the last set bit, then naturally constructed ConciseSet won't have the last word as zero
    // sequence, it will be a literal or one sequence.
    int zeroSequence = 1; // Zero sequence of length 62
    ConciseSet set1 = new ConciseSet(new int[] {zeroSequence}, false);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set2.add(i);
    }
    verifyIntersection(set1, set2);
  }

  @Test
  public void testIntersectionOneFillAndOneFillWithFlipBit()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 100; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (i != 2) {
        set2.add(i);
      }
    }
    verifyIntersection(set1, set2);
  }

  @Test
  public void testIntersectionSecondOneFillRemovedFromQueue()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 31 * 2; i++) {
      set1.add(i);
    }
    set1.add(100);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 31 * 3; i++) {
      set2.add(i);
    }

    verifyIntersection(set1, set2);
  }

  @Test
  public void testIntersectionFirstOneFillRemovedFromQueue()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 31 * 2; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 31 * 3; i++) {
      set2.add(i);
    }

    verifyIntersection(set1, set2);
  }

  @Test
  public void testIntersectionTerminates() throws Exception
  {
    verifyIntersection(Collections.emptyList(), Arrays.asList(new ImmutableConciseSet(), new ImmutableConciseSet()));
  }

  private static List<Integer> toList(ConciseSet set)
  {
    List<Integer> list1 = new ArrayList<>();
    for (IntSet.IntIterator it = set.iterator(); it.hasNext(); ) {
      list1.add(it.next());
    }
    return list1;
  }

  private void verifyIntersection(ConciseSet set1, ConciseSet set2)
  {
    List<Integer> expectedIntersection = toList(set1);
    expectedIntersection.retainAll(toList(set2));
    verifyIntersection(expectedIntersection, set1, set2);
  }

  private void verifyIntersection(List<Integer> expected, ConciseSet set1, ConciseSet set2)
  {
    ImmutableConciseSet immutableSet1 = ImmutableConciseSet.newImmutableFromMutable(set1);
    ImmutableConciseSet immutableSet2 = ImmutableConciseSet.newImmutableFromMutable(set2);
    if (compact) {
      immutableSet1 = ImmutableConciseSet.compact(immutableSet1);
      immutableSet2 = ImmutableConciseSet.compact(immutableSet2);
    }
    List<ImmutableConciseSet> immutableSets = Arrays.asList(immutableSet1, immutableSet2);
    verifyIntersection(expected, immutableSets);
  }

  private void verifyIntersection(List<Integer> expected, List<ImmutableConciseSet> sets)
  {
    List<Integer> actual = Lists.newArrayList();
    ImmutableConciseSet set = ImmutableConciseSet.intersection(sets);
    IntSet.IntIterator itr = set.iterator();
    while (itr.hasNext()) {
      actual.add(itr.next());
    }
    Assert.assertEquals(expected, actual);
  }
}
