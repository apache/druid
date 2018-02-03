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

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Longs;
import io.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MergingRowIteratorTest
{
  @Test
  public void testEmpty()
  {
    testMerge();
  }

  @Test
  public void testOneEmptyIterator()
  {
    testMerge(Collections.emptyList());
  }

  @Test
  public void testMultipleEmptyIterators()
  {
    testMerge(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testConstantIterator()
  {
    testMerge(Longs.asList(1, 1, 1));
  }

  @Test
  public void testMultipleConstantIterators()
  {
    testMerge(Longs.asList(2, 2, 2), Longs.asList(3, 3, 3), Longs.asList(1, 1, 1));
  }

  @Test
  public void testAllPossible5ElementSequences()
  {
    List<List<Long>> possibleSequences = new ArrayList<>();
    populateSequences(possibleSequences, new ArrayDeque<>(), 1, 6, 5);
    for (int i1 = 0; i1 < possibleSequences.size(); i1++) {
      for (int i2 = i1; i2 < possibleSequences.size(); i2++) {
        for (int i3 = i2; i3 < possibleSequences.size(); i3++) {
          testMerge(possibleSequences.get(i1), possibleSequences.get(i2), possibleSequences.get(i3));
        }
      }
    }
  }

  private static void populateSequences(
      List<List<Long>> possibleSequences,
      Deque<Long> currentSequence,
      int current,
      int max,
      int maxDepth
  )
  {
    possibleSequences.add(new ArrayList<>(currentSequence));
    if (currentSequence.size() == maxDepth) {
      return;
    }
    for (int i = current; i < max; i++) {
      currentSequence.addLast((long) i);
      populateSequences(possibleSequences, currentSequence, i, max, maxDepth);
      currentSequence.removeLast();
    }
  }

  @SafeVarargs
  private static void testMerge(List<Long>... timestampSequences)
  {
    String message = Stream.of(timestampSequences).map(List::toString).collect(Collectors.joining(" "));
    int totalLength = Stream.of(timestampSequences).mapToInt(List::size).sum();
    for (int markIteration = 0; markIteration < totalLength; markIteration++) {
      testMerge(message, markIteration, timestampSequences);
    }
  }

  @SafeVarargs
  private static void testMerge(String message, int markIteration, List<Long>... timestampSequences)
  {
    MergingRowIterator mergingRowIterator = new MergingRowIterator(
        Stream.of(timestampSequences).map(TestRowIterator::new).collect(Collectors.toList())
    );
    UnmodifiableIterator<Long> mergedTimestamps = Iterators.mergeSorted(
        Stream.of(timestampSequences).map(List::iterator).collect(Collectors.toList()),
        Comparator.naturalOrder()
    );
    long markedTimestamp = 0;
    long currentTimestamp = 0;
    int i = 0;
    boolean marked = false;
    boolean iterated = false;
    while (mergedTimestamps.hasNext()) {
      currentTimestamp = mergedTimestamps.next();
      Assert.assertTrue(message, mergingRowIterator.moveToNext());
      iterated = true;
      Assert.assertEquals(message, currentTimestamp, mergingRowIterator.getPointer().timestampSelector.getLong());
      if (marked) {
        Assert.assertEquals(
            message,
            markedTimestamp != currentTimestamp,
            mergingRowIterator.hasTimeAndDimsChangedSinceMark()
        );
      }
      if (i == markIteration) {
        mergingRowIterator.mark();
        markedTimestamp = currentTimestamp;
        marked = true;
      }
      i++;
    }
    Assert.assertFalse(message, mergingRowIterator.moveToNext());
    if (iterated) {
      Assert.assertEquals(message, currentTimestamp, mergingRowIterator.getPointer().timestampSelector.getLong());
    }
  }

  private static class TestRowIterator implements TransformableRowIterator
  {
    private final Iterator<Long> timestamps;
    private final RowPointer rowPointer;
    private final SettableLongColumnValueSelector currentTimestamp = new SettableLongColumnValueSelector();
    private final RowNumCounter rowNumCounter = new RowNumCounter();
    private final SettableLongColumnValueSelector markedTimestamp = new SettableLongColumnValueSelector();
    private final TimeAndDimsPointer markedRowPointer;

    private TestRowIterator(Iterable<Long> timestamps)
    {
      this.timestamps = timestamps.iterator();
      this.rowPointer = new RowPointer(
          currentTimestamp,
          ColumnValueSelector.EMPTY_ARRAY,
          Collections.emptyList(),
          ColumnValueSelector.EMPTY_ARRAY,
          Collections.emptyList(),
          rowNumCounter
      );
      this.markedRowPointer = new TimeAndDimsPointer(
          markedTimestamp,
          ColumnValueSelector.EMPTY_ARRAY,
          Collections.emptyList(),
          ColumnValueSelector.EMPTY_ARRAY,
          Collections.emptyList()
      );
    }

    @Override
    public void mark()
    {
      markedTimestamp.setValueFrom(currentTimestamp);
    }

    @Override
    public boolean hasTimeAndDimsChangedSinceMark()
    {
      return markedTimestamp.getLong() != currentTimestamp.getLong();
    }

    @Override
    public RowPointer getPointer()
    {
      return rowPointer;
    }

    @Override
    public TimeAndDimsPointer getMarkedPointer()
    {
      return markedRowPointer;
    }

    @Override
    public boolean moveToNext()
    {
      if (!timestamps.hasNext()) {
        return false;
      }
      currentTimestamp.setValue(timestamps.next());
      rowNumCounter.increment();
      return true;
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
