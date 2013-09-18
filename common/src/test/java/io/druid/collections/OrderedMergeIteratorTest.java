/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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
}
