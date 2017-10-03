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

import io.druid.java.util.common.IAE;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.longs.LongHeaps;

import java.util.List;
import java.util.NoSuchElementException;

public final class IntIteratorUtils
{
  /**
   * Implements {@link IntIterator#skip(int)}.
   */
  public static int skip(IntIterator it, int n)
  {
    if (n < 0) {
      throw new IAE("n: " + n);
    }
    int skipped = 0;
    while (skipped < n && it.hasNext()) {
      it.nextInt();
      skipped++;
    }
    return skipped;
  }

  /**
   * Merges several iterators of ascending {@code int} values into a single iterator of ascending {@code int} values.
   * It isn't checked if the given source iterators are actually ascending, if they are not, the order of values in the
   * returned iterator is undefined.
   * <p>
   * This is similar to what {@link io.druid.java.util.common.guava.MergeIterator} does with simple
   * {@link java.util.Iterator}s.
   *
   * @param iterators iterators to merge, must return ascending values
   */
  public static IntIterator mergeAscending(List<IntIterator> iterators)
  {
    if (iterators.isEmpty()) {
      return IntIterators.EMPTY_ITERATOR;
    }
    if (iterators.size() == 1) {
      return iterators.get(0);
    }
    return new MergeIntIterator(iterators);
  }

  /**
   * This class is designed mostly after {@link io.druid.java.util.common.guava.MergeIterator}.
   * {@code MergeIterator} uses a priority queue of wrapper
   * "peeking" iterators. Peeking wrappers are not available in fastutil for specialized iterators like IntIterator, so
   * they should be implemented manually in the druid codebase. Instead, another approach is taken: a priority queue
   * of primitive long values is used, in long values the high 32-bits is the last value from some iterator, and the low
   * 32 bits is the index of this iterator in the given list (copied to array, to avoid indirection during the
   * iteration) of source iterators. Since values are in the high bits, the composed longs are still compared using the
   * natural order. So this approach avoids indirections and implementing PeekingIntIterator.
   * <p>
   * Instead of {@link it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue}, a priority queue is implemented on lower
   * level, to avoid heap array shuffling on each iteration with dequeue and than enqueue, when merged iterators tend
   * to stay in the head of the heap for at least several iterations.
   */
  static final class MergeIntIterator extends AbstractIntIterator
  {

    private final IntIterator[] iterators;
    private final long[] pQueue;
    private int pQueueSize;

    private static long makeQueueElement(int value, int index)
    {
      // Don't have to mask index because this is a Java array index => positive => no sign bit extension
      return index | (((long) value) << 32);
    }

    private static int value(long queueElement)
    {
      return (int) (queueElement >>> 32);
    }

    private static int iterIndex(long queueElement)
    {
      return (int) queueElement;
    }

    MergeIntIterator(List<IntIterator> iterators)
    {
      this.iterators = iterators.toArray(new IntIterator[0]);
      pQueue = new long[iterators.size()];
      pQueueSize = 0;

      for (int iterIndex = 0; iterIndex < this.iterators.length; iterIndex++) {
        IntIterator iter = this.iterators[iterIndex];
        if (iter != null && iter.hasNext()) {
          pQueue[pQueueSize] = makeQueueElement(iter.nextInt(), iterIndex);
          pQueueSize++;
          LongHeaps.upHeap(pQueue, pQueueSize, pQueueSize - 1, null);
        }
      }

    }

    @Override
    public boolean hasNext()
    {
      return pQueueSize != 0;
    }

    @Override
    public int nextInt()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      long queueHead = pQueue[0];
      int retVal = value(queueHead);
      int iterIndex = iterIndex(queueHead);
      IntIterator retIt = iterators[iterIndex];

      if (retIt.hasNext()) {
        // stay in the head, likely no elements will be moved in the heap
        pQueue[0] = makeQueueElement(retIt.nextInt(), iterIndex);
        LongHeaps.downHeap(pQueue, pQueueSize, 0, null);
      } else {
        pQueueSize--;
        if (pQueueSize != 0) {
          pQueue[0] = pQueue[pQueueSize];
          LongHeaps.downHeap(pQueue, pQueueSize, 0, null);
        }
      }

      return retVal;
    }

    @Override
    public int skip(int n)
    {
      return IntIteratorUtils.skip(this, n);
    }
  }

  public static IntIterator fromRoaringBitmapIntIterator(org.roaringbitmap.IntIterator iterator)
  {
    return new RoaringBitmapDelegatingIntIterator(iterator);
  }

  private static class RoaringBitmapDelegatingIntIterator extends AbstractIntIterator
  {
    private final org.roaringbitmap.IntIterator delegate;

    private RoaringBitmapDelegatingIntIterator(org.roaringbitmap.IntIterator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext();
    }

    @Override
    public int nextInt()
    {
      return delegate.next();
    }

    @Override
    public int skip(int n)
    {
      return IntIteratorUtils.skip(this, n);
    }
  }

  public static IntList toIntList(IntIterator iterator)
  {
    final IntList integers = new IntArrayList();
    while (iterator.hasNext()) {
      integers.add(iterator.nextInt());
    }
    return IntLists.unmodifiable(integers);
  }

  private IntIteratorUtils() {}
}
