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
  public void testFlatMap()
  {
    final CloseableIterator<Integer> actual = generateTestIterator(8)
        .flatMap(list -> CloseableIterators.withEmptyBaggage(list.iterator()));
    final Iterator<Integer> expected = IntStream
        .range(0, 8)
        .flatMap(i -> IntStream.range(0, i))
        .iterator();
    while (expected.hasNext() && actual.hasNext()) {
      Assert.assertEquals(expected.next(), actual.next());
    }
    Assert.assertFalse(actual.hasNext());
    Assert.assertFalse(expected.hasNext());
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
}
