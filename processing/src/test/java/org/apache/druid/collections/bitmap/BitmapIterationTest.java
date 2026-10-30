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

package org.apache.druid.collections.bitmap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.IntIterator;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BitmapIterationTest
{
  public static List<BitmapFactory> factories()
  {
    return List.of(new BitSetBitmapFactory(), new ConciseBitmapFactory());
  }

  public static Stream<Arguments> factoriesAndElements()
  {
    final List<List<Integer>> elementSets = List.of(
        List.of(),
        List.of(1),
        List.of(1, 0, 2, 3, 4),
        List.of(4, 3, 2, 1, 0, 5, 6, 7, 8, 9),
        List.of(1, 0, 2, 3, 4, 1, 0),
        IntStream.range(0, 128).boxed().collect(Collectors.toList())
    );

    return factories().stream()
                      .flatMap(factory -> elementSets.stream().map(elements -> Arguments.of(factory, elements)));
  }

  @ParameterizedTest
  @MethodSource("factoriesAndElements")
  public void testIteration(final BitmapFactory factory, final List<Integer> elements)
  {
    final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    elements.forEach(mutableBitmap::add);

    final BitmapCollection collection = new BitmapCollection(factory.makeImmutableBitmap(mutableBitmap));
    final List<Integer> expectedElements = elements.stream().distinct().sorted().collect(Collectors.toList());
    final List<Integer> actualElements = new ArrayList<>();
    final Iterator<Integer> iterator = collection.iterator();
    while (iterator.hasNext()) {
      actualElements.add(iterator.next());
    }

    Assertions.assertEquals(expectedElements, actualElements);
    Assertions.assertEquals(expectedElements.size(), collection.size());
    Assertions.assertFalse(iterator.hasNext());
    Assertions.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @ParameterizedTest
  @MethodSource("factories")
  public void testRestrictedElements(final BitmapFactory factory)
  {
    final BitmapCollection collection = new BitmapCollection(factory.makeEmptyImmutableBitmap());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> collection.add(0));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> collection.addAll(List.of(0)));
    Assertions.assertThrows(UnsupportedOperationException.class, collection.iterator()::remove);
  }

  private static class BitmapCollection extends AbstractCollection<Integer>
  {
    private final ImmutableBitmap bitmap;

    private BitmapCollection(final ImmutableBitmap bitmap)
    {
      this.bitmap = bitmap;
    }

    @Override
    public Iterator<Integer> iterator()
    {
      final IntIterator iterator = bitmap.iterator();
      return new Iterator<>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public Integer next()
        {
          return iterator.next();
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int size()
    {
      return bitmap.size();
    }
  }
}
