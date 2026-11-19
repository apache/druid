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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.GenericIndexed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ComparatorSortedDimensionDictionaryTest
{
  @Test
  public void testStringsWithNull()
  {
    final List<String> idToValue = Arrays.asList("b", null, "c", "a");
    final ComparatorSortedDimensionDictionary<String> dictionary =
        new ComparatorSortedDimensionDictionary<>(idToValue, GenericIndexed.STRING_STRATEGY, 4);

    assertDictionary(idToValue, 4, dictionary, Arrays.asList(null, "a", "b", "c"));
    Assertions.assertEquals(1, dictionary.getUnsortedIdFromSortedId(0));
    Assertions.assertEquals(3, dictionary.getUnsortedIdFromSortedId(1));
    Assertions.assertEquals(0, dictionary.getUnsortedIdFromSortedId(2));
    Assertions.assertEquals(2, dictionary.getUnsortedIdFromSortedId(3));
  }

  @Test
  public void testStringsSortInJavaStringOrder()
  {
    // U+FFFD sorts after U+1F600 in Java String order but before it in UTF-8 byte order. STRING_STRATEGY uses Java
    // String order, consistent with GenericIndexed.UTF8_STRATEGY, which compares UTF-8 bytes using Java String order.
    final String replacementChar = "\uFFFD";
    final String emoji = "\uD83D\uDE00";
    Assertions.assertTrue(Arrays.compareUnsigned(StringUtils.toUtf8(replacementChar), StringUtils.toUtf8(emoji)) < 0);

    final List<String> idToValue = Arrays.asList(replacementChar, null, emoji, "a");
    assertDictionary(
        idToValue,
        4,
        new ComparatorSortedDimensionDictionary<>(idToValue, GenericIndexed.STRING_STRATEGY, 4),
        Arrays.asList(null, "a", emoji, replacementChar)
    );
  }

  @Test
  public void testLongs()
  {
    final List<Long> idToValue = Arrays.asList(5L, -3L, null, 0L, Long.MAX_VALUE, Long.MIN_VALUE);
    assertDictionary(
        idToValue,
        6,
        new ComparatorSortedDimensionDictionary<>(idToValue, ColumnType.LONG.getNullableStrategy(), 6),
        Arrays.asList(null, Long.MIN_VALUE, -3L, 0L, 5L, Long.MAX_VALUE)
    );
  }

  @Test
  public void testDoubles()
  {
    final List<Double> idToValue = Arrays.asList(1.5, null, -2.25, 0.0, 100.0);
    assertDictionary(
        idToValue,
        5,
        new ComparatorSortedDimensionDictionary<>(idToValue, ColumnType.DOUBLE.getNullableStrategy(), 5),
        Arrays.asList(null, -2.25, 0.0, 1.5, 100.0)
    );
  }

  @Test
  public void testNullsLastComparator()
  {
    final List<String> idToValue = Arrays.asList("b", null, "c", "a");
    assertDictionary(
        idToValue,
        4,
        new ComparatorSortedDimensionDictionary<>(idToValue, Comparator.nullsLast(Comparator.naturalOrder()), 4),
        Arrays.asList("a", "b", "c", null)
    );
  }

  @Test
  public void testReverseComparator()
  {
    final List<String> idToValue = Arrays.asList("b", null, "c", "a");
    assertDictionary(
        idToValue,
        4,
        new ComparatorSortedDimensionDictionary<>(
            idToValue,
            Comparator.nullsFirst(Comparator.<String>reverseOrder()),
            4
        ),
        Arrays.asList(null, "c", "b", "a")
    );
  }

  @Test
  public void testOnlyNull()
  {
    final List<String> idToValue = Collections.singletonList(null);
    assertDictionary(
        idToValue,
        1,
        new ComparatorSortedDimensionDictionary<>(idToValue, GenericIndexed.STRING_STRATEGY, 1),
        Collections.singletonList(null)
    );
  }

  @Test
  public void testLengthSmallerThanList()
  {
    final List<String> idToValue = Arrays.asList("c", null, "a", "b", "z");
    final ComparatorSortedDimensionDictionary<String> dictionary =
        new ComparatorSortedDimensionDictionary<>(idToValue, GenericIndexed.STRING_STRATEGY, 4);

    assertDictionary(idToValue, 4, dictionary, Arrays.asList(null, "a", "b", "c"));
    Assertions.assertThrows(IndexOutOfBoundsException.class, () -> dictionary.getValueFromSortedId(4));
  }

  @Test
  public void testSortFromDimensionDictionary()
  {
    final ComparatorDimensionDictionary<String> dictionary =
        new ComparatorDimensionDictionary<>(GenericIndexed.STRING_STRATEGY)
        {
          @Override
          public long estimateSizeOfValue(String value)
          {
            return value.length();
          }
        };
    for (final String value : Arrays.asList("c", null, "a", "b", "a", null, "c")) {
      dictionary.add(value);
    }

    final ComparatorSortedDimensionDictionary<String> sorted = dictionary.sort();
    final List<String> expected = Arrays.asList(null, "a", "b", "c");
    for (int index = 0; index < expected.size(); index++) {
      final String value = expected.get(index);
      Assertions.assertEquals(value, sorted.getValueFromSortedId(index));
      Assertions.assertEquals(dictionary.getId(value), sorted.getUnsortedIdFromSortedId(index));
      Assertions.assertEquals(index, sorted.getSortedIdFromUnsortedId(dictionary.getId(value)));
    }
    Assertions.assertThrows(IndexOutOfBoundsException.class, () -> sorted.getValueFromSortedId(expected.size()));
  }

  @Test
  public void testRandomStrings()
  {
    final Random random = new Random(42);
    final Set<String> seen = new HashSet<>();
    final List<String> idToValue = new ArrayList<>();
    while (idToValue.size() < 1000) {
      final String value = Integer.toString(random.nextInt(), 36);
      if (seen.add(value)) {
        idToValue.add(value);
      }
    }
    idToValue.add(random.nextInt(idToValue.size()), null);

    final List<String> expected = new ArrayList<>(idToValue);
    expected.sort(GenericIndexed.STRING_STRATEGY);
    assertDictionary(
        idToValue,
        idToValue.size(),
        new ComparatorSortedDimensionDictionary<>(idToValue, GenericIndexed.STRING_STRATEGY, idToValue.size()),
        expected
    );
  }

  private static <T> void assertDictionary(
      List<T> idToValue,
      int length,
      ComparatorSortedDimensionDictionary<T> dictionary,
      List<T> expectedSortedValues
  )
  {
    for (int index = 0; index < length; index++) {
      Assertions.assertEquals(expectedSortedValues.get(index), dictionary.getValueFromSortedId(index));
      final int id = dictionary.getUnsortedIdFromSortedId(index);
      Assertions.assertEquals(expectedSortedValues.get(index), idToValue.get(id));
      Assertions.assertEquals(index, dictionary.getSortedIdFromUnsortedId(id));
    }
  }
}
