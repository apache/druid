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

package io.druid.segment;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static io.druid.segment.IntIteratorUtils.mergeAscending;
import static it.unimi.dsi.fastutil.ints.IntIterators.EMPTY_ITERATOR;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class MergeIntIteratorTest
{
  @Test(expected = NoSuchElementException.class)
  public void testNoIterators()
  {
    IntIterator it = mergeAscending(Collections.<IntIterator>emptyList());
    assertEmpty(it);
  }

  @Test(expected = NoSuchElementException.class)
  public void testMergeEmptyIterators()
  {
    IntIterator it = mergeAscending(Arrays.<IntIterator>asList(EMPTY_ITERATOR, EMPTY_ITERATOR));
    assertEmpty(it);
  }

  private static void assertEmpty(IntIterator it)
  {
    assertFalse(it.hasNext());
    try {
      it.next();
      fail("expected NoSuchElementException on it.next() after it.hasNext() = false");
    }
    catch (NoSuchElementException ignore) {
      // expected
    }
    // expected to fail with NoSuchElementException
    it.nextInt();
  }

  /**
   * Check for some possible corner cases, because {@link IntIteratorUtils.MergeIntIterator} is
   * implemented using packing ints within longs, that is prone to some overflow or sign bit extension bugs
   */
  @Test
  public void testOverflow()
  {
    List<IntList> lists = Lists.newArrayList(
        IntLists.singleton(Integer.MIN_VALUE),
        IntLists.singleton(Integer.MIN_VALUE),
        IntLists.singleton(-1),
        IntLists.singleton(0),
        IntLists.singleton(MAX_VALUE)
    );
    for (int i = 0; i < lists.size() + 1; i++) {
      assertAscending(mergeAscending(iteratorsFromLists(lists)));
      Collections.rotate(lists, 1);
    }
    Collections.shuffle(lists);
    assertAscending(mergeAscending(iteratorsFromLists(lists)));
  }

  private static List<IntIterator> iteratorsFromLists(List<IntList> lists)
  {
    ArrayList<IntIterator> r = new ArrayList<>();
    for (IntList list : lists) {
      r.add(list.iterator());
    }
    return r;
  }

  @Test
  public void smokeTest()
  {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    for (int i = 0; i < 1000; i++) {
      int numIterators = r.nextInt(1, 11);
      List<IntList> lists = new ArrayList<>(numIterators);
      for (int j = 0; j < numIterators; j++) {
        lists.add(new IntArrayList());
      }
      for (int j = 0; j < 50; j++) {
        lists.get(r.nextInt(numIterators)).add(j);
      }
      for (int j = 0; j < lists.size() + 1; j++) {
        assertAscending(mergeAscending(iteratorsFromLists(lists)));
        Collections.rotate(lists, 1);
      }
      for (int j = 0; j < 10; j++) {
        Collections.shuffle(lists);
        assertAscending(mergeAscending(iteratorsFromLists(lists)));
      }
    }
  }

  private static void assertAscending(IntIterator it)
  {
    if (!it.hasNext()) {
      return;
    }
    int prev = it.nextInt();
    for (; it.hasNext(); ) {
      int current = it.nextInt();
      if (prev > current) {
        Assert.fail("not ascending: " + prev + ", then " + current);
      }
      prev = current;
    }
  }
}
