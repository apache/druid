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

package org.apache.druid.segment;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.data.Indexed;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Iterator for merging dictionaries for some comparable type into a single sorted dictionary, useful when merging
 * dictionary encoded columns
 */
public class DictionaryMergingIterator<T extends Comparable<T>> implements CloseableIterator<T>
{
  private static final Logger log = new Logger(DictionaryMergingIterator.class);

  public static <T extends Comparable<T>> Comparator<Pair<Integer, PeekingIterator<T>>> makePeekingComparator()
  {
    return (lhs, rhs) -> {
      T left = lhs.rhs.peek();
      T right = rhs.rhs.peek();
      if (left == null) {
        //noinspection VariableNotUsedInsideIf
        return right == null ? 0 : -1;
      } else if (right == null) {
        return 1;
      } else {
        return left.compareTo(right);
      }
    };
  }

  protected final IntBuffer[] conversions;
  protected final List<Pair<ByteBuffer, Integer>> directBufferAllocations = new ArrayList<>();
  protected final PriorityQueue<Pair<Integer, PeekingIterator<T>>> pQueue;

  protected int counter;

  public DictionaryMergingIterator(
      Indexed<T>[] dimValueLookups,
      Comparator<Pair<Integer, PeekingIterator<T>>> comparator,
      boolean useDirect
  )
  {
    pQueue = new PriorityQueue<>(dimValueLookups.length, comparator);
    conversions = new IntBuffer[dimValueLookups.length];

    long mergeBufferTotalSize = 0;
    for (int i = 0; i < conversions.length; i++) {
      if (dimValueLookups[i] == null) {
        continue;
      }
      Indexed<T> indexed = dimValueLookups[i];
      if (useDirect) {
        int allocationSize = indexed.size() * Integer.BYTES;
        log.trace("Allocating dictionary merging direct buffer with size[%,d]", allocationSize);
        mergeBufferTotalSize += allocationSize;
        final ByteBuffer conversionDirectBuffer = ByteBuffer.allocateDirect(allocationSize);
        conversions[i] = conversionDirectBuffer.asIntBuffer();
        directBufferAllocations.add(new Pair<>(conversionDirectBuffer, allocationSize));
      } else {
        conversions[i] = IntBuffer.allocate(indexed.size());
        mergeBufferTotalSize += indexed.size();
      }

      final PeekingIterator<T> iter = transformIndexedIterator(indexed);
      if (iter.hasNext()) {
        pQueue.add(Pair.of(i, iter));
      }
    }
    log.debug("Allocated [%,d] bytes of dictionary merging direct buffers", mergeBufferTotalSize);
  }

  @Override
  public boolean hasNext()
  {
    return !pQueue.isEmpty();
  }

  @Override
  public T next()
  {
    Pair<Integer, PeekingIterator<T>> smallest = pQueue.remove();
    if (smallest == null) {
      throw new NoSuchElementException();
    }
    final T value = writeTranslate(smallest, counter);

    while (!pQueue.isEmpty() && Objects.equals(value, pQueue.peek().rhs.peek())) {
      writeTranslate(pQueue.remove(), counter);
    }
    counter++;

    return value;
  }

  public int getCardinality()
  {
    return counter;
  }

  protected PeekingIterator<T> transformIndexedIterator(Indexed<T> indexed)
  {
    return Iterators.peekingIterator(
        indexed.iterator()
    );
  }

  protected boolean needConversion(int index)
  {
    IntBuffer readOnly = conversions[index].asReadOnlyBuffer();
    readOnly.rewind();
    int i = 0;
    while (readOnly.hasRemaining()) {
      if (i != readOnly.get()) {
        return true;
      }
      i++;
    }
    return false;
  }

  protected T writeTranslate(Pair<Integer, PeekingIterator<T>> smallest, int counter)
  {
    final int index = smallest.lhs;
    final T value = smallest.rhs.next();

    conversions[index].put(counter);
    if (smallest.rhs.hasNext()) {
      pQueue.add(smallest);
    }
    return value;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close()
  {
    long mergeBufferTotalSize = 0;
    for (Pair<ByteBuffer, Integer> bufferAllocation : directBufferAllocations) {
      log.trace("Freeing dictionary merging direct buffer with size[%,d]", bufferAllocation.rhs);
      mergeBufferTotalSize += bufferAllocation.rhs;
      ByteBufferUtils.free(bufferAllocation.lhs);
    }
    log.debug("Freed [%,d] bytes of dictionary merging direct buffers", mergeBufferTotalSize);
  }
}
