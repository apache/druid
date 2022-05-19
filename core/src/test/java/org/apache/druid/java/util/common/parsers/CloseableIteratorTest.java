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

package org.apache.druid.java.util.common.parsers;

import org.apache.druid.java.util.common.CloseableIterators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CloseableIteratorTest
{
  @Test
  public void testMap()
  {
    final CloseableIterator<List<Integer>> actual = generateTestIterator(8)
        .map(list -> {
          final List<Integer> newList = new ArrayList<>(list.size());
          for (Integer i : list) {
            newList.add(i * 10);
          }
          return newList;
        });
    final Iterator<List<Integer>> expected = IntStream.range(0, 8)
        .mapToObj(i -> IntStream.range(0, i).map(j -> j * 10).boxed().collect(Collectors.toList()))
        .iterator();
    while (expected.hasNext() && actual.hasNext()) {
      Assert.assertEquals(expected.next(), actual.next());
    }
    Assert.assertFalse(actual.hasNext());
    Assert.assertFalse(expected.hasNext());
  }

  @Test
  public void testFlatMap() throws IOException
  {
    List<CloseTrackingCloseableIterator<Integer>> innerIterators = new ArrayList<>();
    final CloseTrackingCloseableIterator<Integer> actual = new CloseTrackingCloseableIterator<>(
        generateTestIterator(8)
            .flatMap(list -> {
              CloseTrackingCloseableIterator<Integer> inner =
                  new CloseTrackingCloseableIterator<>(CloseableIterators.withEmptyBaggage(list.iterator()));
              innerIterators.add(inner);
              return inner;
            })
    );
    final Iterator<Integer> expected = IntStream
        .range(0, 8)
        .flatMap(i -> IntStream.range(0, i))
        .iterator();
    while (expected.hasNext() && actual.hasNext()) {
      Assert.assertEquals(expected.next(), actual.next());
    }
    Assert.assertFalse(actual.hasNext());
    Assert.assertFalse(expected.hasNext());
    actual.close();
    Assert.assertEquals(1, actual.closeCount);
    for (CloseTrackingCloseableIterator iter : innerIterators) {
      Assert.assertEquals(1, iter.closeCount);
    }
  }

  @Test
  public void testFlatMapClosedEarly() throws IOException
  {
    final int numIterations = 8;
    List<CloseTrackingCloseableIterator<Integer>> innerIterators = new ArrayList<>();
    final CloseTrackingCloseableIterator<Integer> actual = new CloseTrackingCloseableIterator<>(
        generateTestIterator(numIterations)
            .flatMap(list -> {
              CloseTrackingCloseableIterator<Integer> inner =
                  new CloseTrackingCloseableIterator<>(CloseableIterators.withEmptyBaggage(list.iterator()));
              innerIterators.add(inner);
              return inner;
            })
    );
    final Iterator<Integer> expected = IntStream
        .range(0, numIterations)
        .flatMap(i -> IntStream.range(0, i))
        .iterator();

    // burn through the first few iterators
    int cnt = 0;
    int numFlatIterations = 5;
    while (expected.hasNext() && actual.hasNext() && cnt++ < numFlatIterations) {
      Assert.assertEquals(expected.next(), actual.next());
    }
    // but stop while we still have an open current inner iterator and a few remaining inner iterators
    Assert.assertTrue(actual.hasNext());
    Assert.assertTrue(expected.hasNext());
    Assert.assertEquals(4, innerIterators.size());
    Assert.assertTrue(innerIterators.get(innerIterators.size() - 1).hasNext());
    actual.close();
    Assert.assertEquals(1, actual.closeCount);
    for (CloseTrackingCloseableIterator iter : innerIterators) {
      Assert.assertEquals(1, iter.closeCount);
    }
  }

  private static CloseableIterator<List<Integer>> generateTestIterator(int numIterates)
  {
    return new CloseableIterator<List<Integer>>()
    {
      private int cnt = 0;

      @Override
      public boolean hasNext()
      {
        return cnt < numIterates;
      }

      @Override
      public List<Integer> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final List<Integer> integers = IntStream.range(0, cnt).boxed().collect(Collectors.toList());
        cnt++;
        return integers;
      }

      @Override
      public void close()
      {
        // do nothing
      }
    };
  }

  static class CloseTrackingCloseableIterator<T> implements CloseableIterator<T>
  {
    CloseableIterator<T> inner;
    int closeCount;

    public CloseTrackingCloseableIterator(CloseableIterator<T> toTrack)
    {
      this.inner = toTrack;
      this.closeCount = 0;
    }


    @Override
    public void close() throws IOException
    {
      inner.close();
      closeCount++;
    }

    @Override
    public boolean hasNext()
    {
      return inner.hasNext();
    }

    @Override
    public T next()
    {
      return inner.next();
    }
  }
}
