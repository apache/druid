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

package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class OrderedMergeIteratorTest
{
  @Test
  public void testSanity() throws Exception
  {
    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    iterators.add(Arrays.asList(1, 3, 5, 7, 9).iterator());
    iterators.add(Arrays.asList(2, 8).iterator());
    iterators.add(Arrays.asList(4, 6, 8).iterator());

    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );

    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9), Lists.newArrayList(iter));
  }

  @Test
  public void testScrewsUpOnOutOfOrderBeginningOfList() throws Exception
  {
    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    iterators.add(Arrays.asList(1, 3, 5, 7, 9).iterator());
    iterators.add(Arrays.asList(4, 6).iterator());
    iterators.add(Arrays.asList(2, 8).iterator());

    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );

    Assert.assertEquals(Arrays.asList(1, 3, 4, 2, 5, 6, 7, 8, 9), Lists.newArrayList(iter));
  }

  @Test
  public void testScrewsUpOnOutOfOrderInList() throws Exception
  {
    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    iterators.add(Arrays.asList(1, 3, 5, 4, 7, 9).iterator());
    iterators.add(Arrays.asList(2, 8).iterator());
    iterators.add(Arrays.asList(4, 6).iterator());

    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );

    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 4, 6, 7, 8, 9), Lists.newArrayList(iter));
  }

  @Test
  public void testLaziness() throws Exception
  {
    final boolean[] done = new boolean[]{false, false};

    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    iterators.add(
        new IteratorShell<Integer>(Arrays.asList(1, 2, 3).iterator())
        {
          @Override
          public boolean hasNext()
          {
            boolean retVal = super.hasNext();
            if (!retVal) {
              done[0] = true;
            }
            return retVal;
          }
        }
    );
    iterators.add(
        new IteratorShell<Integer>(Arrays.asList(4, 5, 6).iterator())
        {
          int count = 0;

          @Override
          public boolean hasNext()
          {
            if (count >= 1) {
              Assert.assertTrue("First iterator not complete", done[0]);
            }
            boolean retVal = super.hasNext();
            if (!retVal) {
              done[1] = true;
            }
            return retVal;
          }

          @Override
          public Integer next()
          {
            if (count >= 1) {
              Assert.assertTrue("First iterator not complete", done[0]);
            }
            ++count;
            return super.next();
          }
        }
    );

    iterators.add(
        new IteratorShell<Integer>(Arrays.asList(7, 8, 9).iterator())
        {
          int count = 0;

          @Override
          public boolean hasNext()
          {
            if (count >= 1) {
              Assert.assertTrue("Second iterator not complete", done[1]);
            }
            Assert.assertTrue("First iterator not complete", done[0]);
            return super.hasNext();
          }

          @Override
          public Integer next()
          {
            if (count >= 1) {
              Assert.assertTrue("Second iterator not complete", done[1]);
            }
            Assert.assertTrue("First iterator not complete", done[0]);
            ++count;
            return super.next();
          }
        }
    );

    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );

    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), Lists.newArrayList(iter));
  }

  @Test(expected = NoSuchElementException.class)
  public void testNoElementInNext()
  {
    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );
    iter.next();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemove()
  {
    final ArrayList<Iterator<Integer>> iterators = Lists.newArrayList();
    OrderedMergeIterator<Integer> iter = new OrderedMergeIterator<Integer>(
        Ordering.<Integer>natural(),
        iterators.iterator()
    );
    iter.remove();
  }
}
